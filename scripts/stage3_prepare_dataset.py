"""Stage 3.1 — Prepare the ML dataset for distributed demand prediction.

Reads the enriched LinkedIn job postings Parquet table, aggregates job
postings by (time_bucket, country, city, category), derives a binary
``high_demand`` label using a per-category median split, engineers
temporal and lag/rolling features, performs a temporal train/test split,
and persists train/test Parquet datasets to HDFS.

The script is meant to be submitted with ``spark-submit``. All inputs and
outputs are passed as CLI flags so the pipeline is reproducible from
``scripts/stage3.sh``.
"""

import argparse
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Tuple

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType


DEFAULT_ENRICHED_PATH = "/user/team32/linkedin/enriched/linkedin_jobs_enriched"
DEFAULT_ML_BASE = "/user/team32/linkedin/ml"

# Fall back to daily aggregation when the date range is too narrow for
# weekly buckets to give us a meaningful number of lag observations.
MIN_WEEKS_FOR_WEEKLY = 20

# Lag offsets (in time buckets) and rolling-window size used when
# building past-only aggregate features. These mirror common
# seasonality assumptions: last bucket, last month, and a four-bucket
# rolling mean.
LAG_PERIODS = (1, 4)
ROLLING_WINDOW = 4

# Drop (country, position) groups that have fewer than this many time
# buckets of history. Sparse groups make lag/rolling features mostly
# zero-fill noise.
MIN_BUCKETS_PER_GROUP = 5

# Per-category quantile that defines "high demand" relative to the
# typical week for that search position. 0.75 gives a top-quartile
# label that is sharper and more actionable than a 0.5 median split.
HIGH_DEMAND_QUANTILE = 0.75

# Fraction of the (ordered) time range to assign to the training split.
TRAIN_FRACTION = 0.8

EPOCH = date(1970, 1, 1)


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--enriched-path",
        default=DEFAULT_ENRICHED_PATH,
        help="HDFS path to the enriched job postings Parquet table.",
    )
    parser.add_argument(
        "--ml-base",
        default=DEFAULT_ML_BASE,
        help="HDFS base path under which dataset_train/ and dataset_test/ are written.",
    )
    parser.add_argument(
        "--output-dir",
        default="output",
        help="Local directory for small CSV/TXT summary artifacts.",
    )
    return parser.parse_args()


def build_spark() -> SparkSession:
    """Create the Spark session used by this stage."""
    return (
        SparkSession.builder
        .appName("team32-stage3-prepare-dataset")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def load_clean_enriched(spark: SparkSession, path: str) -> DataFrame:
    """Read the enriched table and drop rows missing aggregation keys."""
    df = spark.read.parquet(path)
    df = df.filter(
        F.col("first_seen").isNotNull()
        & F.col("search_country").isNotNull()
        & F.col("search_position").isNotNull()
    )
    # Normalise the string keys so the same group is not split because of
    # whitespace or casing differences.
    for col_name in ("search_country", "search_position"):
        df = df.withColumn(col_name, F.trim(F.lower(F.col(col_name))))
    return df


def decide_bucket_mode(df: DataFrame) -> str:
    """Choose between weekly and daily aggregation based on date coverage."""
    week_count = (
        df.select(
            F.year("first_seen").alias("y"),
            F.weekofyear("first_seen").alias("w"),
        )
        .distinct()
        .count()
    )
    return "weekly" if week_count >= MIN_WEEKS_FOR_WEEKLY else "daily"


def add_time_bucket(df: DataFrame, mode: str) -> DataFrame:
    """Attach a normalised ``time_bucket`` date column to the dataframe."""
    if mode == "weekly":
        # date_trunc('week', d) gives the Monday of d's week.
        return df.withColumn(
            "time_bucket",
            F.date_trunc("week", F.col("first_seen").cast("timestamp")).cast("date"),
        )
    return df.withColumn("time_bucket", F.col("first_seen").cast("date"))


def aggregate_groups(df: DataFrame) -> DataFrame:
    """Aggregate raw postings to one row per (bucket, country, category).

    ``search_city`` is intentionally not part of the grouping key. At
    city granularity the vast majority of (country, city, category)
    cells have only one or two observations, which makes lag and
    rolling features essentially zero. Collapsing across cities gives
    denser per-group histories at the cost of losing city-level
    resolution, which is the right trade-off for demand prediction.
    """
    keys = ["time_bucket", "search_country", "search_position"]

    base = df.groupBy(*keys).agg(
        F.count(F.lit(1)).alias("group_count"),
        F.countDistinct("company").alias("distinct_companies"),
        F.avg(
            F.when(F.col("got_summary") == 1, F.lit(1.0)).otherwise(F.lit(0.0))
        ).alias("summary_coverage"),
    )

    skills = (
        df.filter(F.col("job_skills").isNotNull())
        .withColumn("skill", F.explode(F.split(F.col("job_skills"), ",")))
        .withColumn("skill", F.trim(F.lower(F.col("skill"))))
        .filter(F.col("skill") != "")
        .groupBy(*keys)
        .agg(F.countDistinct("skill").alias("distinct_skills"))
    )

    return (
        base.join(skills, keys, "left")
        .fillna({"distinct_skills": 0})
    )


def bucket_counts_per_group(df: DataFrame) -> DataFrame:
    """Return the number of distinct time buckets per (country, position)."""
    return (
        df.groupBy("search_country", "search_position")
        .agg(F.countDistinct("time_bucket").alias("n_buckets"))
    )


def filter_sparse_groups(df: DataFrame, min_buckets: int) -> DataFrame:
    """Keep only (country, position) groups with enough time history."""
    keep = (
        bucket_counts_per_group(df)
        .filter(F.col("n_buckets") >= F.lit(min_buckets))
        .select("search_country", "search_position")
    )
    return df.join(F.broadcast(keep), ["search_country", "search_position"], "inner")


def adaptive_sparse_filter(
    df: DataFrame, target_min_buckets: int
) -> Tuple[DataFrame, int, int]:
    """Apply ``filter_sparse_groups`` and fall back to lower thresholds.

    Some data dumps cover a very narrow time range. If the requested
    ``target_min_buckets`` would wipe the dataset, walk the threshold
    down to 1 (no-op) so we still produce something to train on. The
    resulting dataset will have weak lag signal, but the script logs
    the actual threshold used so the operator can see what happened.
    """
    used = target_min_buckets
    while used > 0:
        filtered = filter_sparse_groups(df, used)
        kept = filtered.count()
        if kept > 0:
            return filtered, used, kept
        used -= 1
    return df.limit(0), 0, 0


def add_temporal_features(df: DataFrame) -> DataFrame:
    """Derive deterministic temporal features from ``time_bucket``."""
    return (
        df.withColumn("year", F.year("time_bucket"))
        .withColumn("month", F.month("time_bucket"))
        .withColumn("week_of_year", F.weekofyear("time_bucket"))
        .withColumn("day_of_week", F.dayofweek("time_bucket"))
        .withColumn("quarter", F.quarter("time_bucket"))
        .withColumn(
            "is_weekend",
            F.when(F.col("day_of_week").isin(1, 7), F.lit(1)).otherwise(F.lit(0)),
        )
    )


def add_lag_features(df: DataFrame) -> DataFrame:
    """Add lag and rolling-mean features over each (country, category) group.

    The values are computed strictly from past time buckets so they can
    be used safely at prediction time without leaking the current count.
    """
    group_window = (
        Window.partitionBy("search_country", "search_position")
        .orderBy("time_bucket")
    )
    rolling_window = group_window.rowsBetween(-ROLLING_WINDOW, -1)

    out = df
    for lag in LAG_PERIODS:
        out = out.withColumn(
            f"lag_count_{lag}",
            F.lag("group_count", lag).over(group_window),
        )
    out = out.withColumn(
        "lag_distinct_companies_1",
        F.lag("distinct_companies", 1).over(group_window),
    )
    out = out.withColumn(
        "lag_distinct_skills_1",
        F.lag("distinct_skills", 1).over(group_window),
    )
    out = out.withColumn(
        f"rolling_mean_count_{ROLLING_WINDOW}",
        F.avg("group_count").over(rolling_window),
    )

    # First rows in each group have NULL lags; fall back to 0 so the
    # feature vector stays usable for Naive Bayes (which forbids nulls).
    lag_cols = [c for c in out.columns if c.startswith("lag_") or c.startswith("rolling_")]
    return out.fillna(0, subset=lag_cols)


def add_high_demand_label(df: DataFrame) -> DataFrame:
    """Derive a per-category top-quantile ``high_demand`` binary label.

    The threshold is the ``HIGH_DEMAND_QUANTILE`` quantile of
    ``group_count`` within each ``search_position``. A row is labelled
    ``1`` if its count exceeds the threshold for its category and ``0``
    otherwise. This produces a roughly (1 - q):q class balance per
    category — natural class imbalance that downstream weighting can
    address if needed.
    """
    thresholds = (
        df.groupBy("search_position")
        .agg(
            F.expr(
                "percentile_approx(group_count, {q})".format(q=HIGH_DEMAND_QUANTILE)
            ).alias("cat_threshold")
        )
    )
    joined = df.join(F.broadcast(thresholds), "search_position")
    return joined.withColumn(
        "high_demand",
        F.when(F.col("group_count") > F.col("cat_threshold"), F.lit(1))
        .otherwise(F.lit(0))
        .cast(IntegerType()),
    ).drop("cat_threshold")


def temporal_split(df: DataFrame) -> Tuple[DataFrame, DataFrame, date]:
    """Split the dataframe so the oldest ``TRAIN_FRACTION`` of time goes to train."""
    days = df.select(F.datediff(F.col("time_bucket"), F.lit(EPOCH.isoformat())).alias("d"))
    cutoff_days = days.approxQuantile("d", [TRAIN_FRACTION], 0.001)[0]
    cutoff_date = EPOCH + timedelta(days=int(cutoff_days))
    cutoff_lit = F.lit(cutoff_date.isoformat()).cast("date")
    train = df.filter(F.col("time_bucket") < cutoff_lit)
    test = df.filter(F.col("time_bucket") >= cutoff_lit)
    return train, test, cutoff_date


def write_distribution(df: DataFrame, split_name: str, out_path: Path) -> None:
    """Write a small CSV with the class balance for ``df``."""
    pdf = (
        df.groupBy("high_demand").count().orderBy("high_demand").toPandas()
    )
    pdf["split"] = split_name
    pdf.to_csv(out_path, index=False)


def main() -> None:
    """Entry point for ``spark-submit``."""
    args = parse_args()
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    print(f"[stage3.prepare] Reading enriched parquet: {args.enriched_path}", flush=True)
    raw = load_clean_enriched(spark, args.enriched_path)

    # Diagnostic: what does first_seen actually look like? If the source
    # dump only covers a few days we want to see that explicitly before
    # any aggregation/filter strips information.
    fs_stats = raw.agg(
        F.min("first_seen").alias("min_fs"),
        F.max("first_seen").alias("max_fs"),
        F.countDistinct("first_seen").alias("distinct_fs"),
    ).first()
    print(
        "[stage3.prepare] Raw first_seen: "
        f"{fs_stats['min_fs']} -> {fs_stats['max_fs']} "
        f"({fs_stats['distinct_fs']} distinct days)",
        flush=True,
    )

    mode = decide_bucket_mode(raw)
    print(f"[stage3.prepare] Time bucket mode: {mode}", flush=True)

    bucketed = add_time_bucket(raw, mode)

    agg = aggregate_groups(bucketed)
    rows_before_filter = agg.count()

    # Diagnostic: distribution of bucket-counts per (country, position)
    # group. This makes it obvious why a particular min_buckets threshold
    # does or does not survive.
    n_groups = bucket_counts_per_group(agg).count()
    bucket_stats = bucket_counts_per_group(agg).agg(
        F.min("n_buckets").alias("min"),
        F.expr("percentile_approx(n_buckets, 0.5)").alias("p50"),
        F.expr("percentile_approx(n_buckets, 0.75)").alias("p75"),
        F.max("n_buckets").alias("max"),
    ).first()
    print(
        f"[stage3.prepare] (country, position) groups: {n_groups}; "
        f"buckets-per-group min={bucket_stats['min']}, p50={bucket_stats['p50']}, "
        f"p75={bucket_stats['p75']}, max={bucket_stats['max']}",
        flush=True,
    )

    agg, used_min_buckets, rows_after_filter = adaptive_sparse_filter(
        agg, MIN_BUCKETS_PER_GROUP
    )
    if used_min_buckets != MIN_BUCKETS_PER_GROUP:
        print(
            f"[stage3.prepare] WARN: relaxed min_buckets from "
            f"{MIN_BUCKETS_PER_GROUP} to {used_min_buckets} so the dataset "
            "is non-empty (lag features will be weaker than intended)",
            flush=True,
        )
    print(
        f"[stage3.prepare] Sparse-group filter (min_buckets={used_min_buckets}): "
        f"{rows_before_filter} -> {rows_after_filter} rows",
        flush=True,
    )
    if rows_after_filter == 0:
        print(
            "[stage3.prepare] FATAL: no rows survived even with min_buckets=1. "
            "Inspect the first_seen distribution above; the temporal pipeline "
            "cannot proceed.",
            flush=True,
        )
        spark.stop()
        sys.exit(1)

    agg = add_temporal_features(agg)
    agg = add_lag_features(agg)
    agg = add_high_demand_label(agg)
    agg.cache()

    bounds = agg.agg(
        F.min("time_bucket").alias("min_bucket"),
        F.max("time_bucket").alias("max_bucket"),
        F.count(F.lit(1)).alias("group_rows"),
    ).first()
    print(
        f"[stage3.prepare] Date range: {bounds['min_bucket']} -> {bounds['max_bucket']}; "
        f"group rows: {bounds['group_rows']}",
        flush=True,
    )

    train, test, cutoff_date = temporal_split(agg)
    train_count = train.count()
    test_count = test.count()
    print(
        f"[stage3.prepare] Temporal split cutoff: {cutoff_date}; "
        f"train rows: {train_count}; test rows: {test_count}",
        flush=True,
    )

    train_path = f"{args.ml_base}/dataset_train"
    test_path = f"{args.ml_base}/dataset_test"
    print(f"[stage3.prepare] Writing train -> {train_path}", flush=True)
    train.write.mode("overwrite").parquet(train_path)
    print(f"[stage3.prepare] Writing test  -> {test_path}", flush=True)
    test.write.mode("overwrite").parquet(test_path)

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    write_distribution(train, "train", out_dir / "stage3_class_distribution_train.csv")
    write_distribution(test, "test", out_dir / "stage3_class_distribution_test.csv")

    summary_path = out_dir / "stage3_prepare_summary.txt"
    with summary_path.open("w", encoding="utf-8") as fh:
        fh.write(f"bucket_mode={mode}\n")
        fh.write(f"high_demand_quantile={HIGH_DEMAND_QUANTILE}\n")
        fh.write(f"min_buckets_per_group_target={MIN_BUCKETS_PER_GROUP}\n")
        fh.write(f"min_buckets_per_group_used={used_min_buckets}\n")
        fh.write(f"raw_min_first_seen={fs_stats['min_fs']}\n")
        fh.write(f"raw_max_first_seen={fs_stats['max_fs']}\n")
        fh.write(f"raw_distinct_first_seen={fs_stats['distinct_fs']}\n")
        fh.write(f"groups_country_position={n_groups}\n")
        fh.write(f"buckets_per_group_min={bucket_stats['min']}\n")
        fh.write(f"buckets_per_group_p50={bucket_stats['p50']}\n")
        fh.write(f"buckets_per_group_p75={bucket_stats['p75']}\n")
        fh.write(f"buckets_per_group_max={bucket_stats['max']}\n")
        fh.write(f"rows_before_filter={rows_before_filter}\n")
        fh.write(f"rows_after_filter={rows_after_filter}\n")
        fh.write(f"min_time_bucket={bounds['min_bucket']}\n")
        fh.write(f"max_time_bucket={bounds['max_bucket']}\n")
        fh.write(f"group_rows={bounds['group_rows']}\n")
        fh.write(f"split_cutoff={cutoff_date}\n")
        fh.write(f"train_rows={train_count}\n")
        fh.write(f"test_rows={test_count}\n")
        fh.write(f"train_path={train_path}\n")
        fh.write(f"test_path={test_path}\n")
    print(f"[stage3.prepare] Summary written to {summary_path}", flush=True)

    spark.stop()


if __name__ == "__main__":
    main()
