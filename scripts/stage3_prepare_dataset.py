"""Stage 3.1 — Prepare the ML dataset for demand classification.

Reads the enriched LinkedIn job postings Parquet, aggregates by
``(search_country, search_position)``, derives a binary ``high_demand``
label via a per-position top-quartile split of the posting count, adds
a few leak-light derived numeric features, performs a random
train/test split, and writes both splits to HDFS as Parquet.

There is no temporal logic in this stage. Both timestamp columns in
the source dump (``first_seen``, ``last_processed_time``) are
scraper-collection artifacts that span only a handful of days and
carry no useful signal — they were removed to keep the pipeline
honest about what it actually learns from (country/position identity
plus aggregated job-market characteristics).
"""

import argparse
import sys
from pathlib import Path
from typing import Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType


DEFAULT_ENRICHED_PATH = "/user/team32/linkedin/enriched/linkedin_jobs_enriched"
DEFAULT_ML_BASE = "/user/team32/linkedin/ml"

# Minimum number of postings a (country, position) group must have to
# be kept. Smaller groups produce noisy aggregated statistics.
MIN_POSTINGS_PER_GROUP = 5

# Per-position quantile of ``group_count`` that defines "high demand"
# relative to the typical demand for that position.
HIGH_DEMAND_QUANTILE = 0.75

# Fraction of (country, position) groups assigned to the training
# split. The remainder is the held-out test set.
TRAIN_FRACTION = 0.8

RANDOM_SEED = 42


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--enriched-path", default=DEFAULT_ENRICHED_PATH)
    parser.add_argument("--ml-base", default=DEFAULT_ML_BASE)
    parser.add_argument("--output-dir", default="output")
    return parser.parse_args()


def build_spark() -> SparkSession:
    """Create the Spark session used by this stage."""
    return (
        SparkSession.builder
        .appName("team32-stage3-prepare-dataset")
        .getOrCreate()
    )


def load_clean_enriched(spark: SparkSession, path: str) -> DataFrame:
    """Read the enriched table and drop rows missing the grouping keys."""
    df = spark.read.parquet(path)
    df = df.filter(
        F.col("search_country").isNotNull()
        & F.col("search_position").isNotNull()
    )
    for col_name in ("search_country", "search_position"):
        df = df.withColumn(col_name, F.trim(F.lower(F.col(col_name))))
    return df


def aggregate_groups(df: DataFrame) -> DataFrame:
    """Aggregate raw postings into one row per (country, position)."""
    keys = ["search_country", "search_position"]

    base = df.groupBy(*keys).agg(
        F.count(F.lit(1)).alias("group_count"),
        F.countDistinct("company").alias("distinct_companies"),
        F.countDistinct("job_level").alias("distinct_job_levels"),
        F.countDistinct("job_type").alias("distinct_job_types"),
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


def filter_small_groups(df: DataFrame, min_count: int) -> DataFrame:
    """Drop (country, position) groups with fewer than ``min_count`` postings."""
    return df.filter(F.col("group_count") >= F.lit(min_count))


def add_derived_features(df: DataFrame) -> DataFrame:
    """Add ratio features that do not directly leak the posting count.

    ``distinct_companies`` and ``distinct_skills`` are correlated with
    ``group_count`` (their upper bound is the count), so we convert
    them to per-posting ratios that describe *how concentrated* the
    market is rather than *how big* it is.
    """
    safe_count = F.greatest(F.col("group_count").cast("double"), F.lit(1.0))
    return (
        df.withColumn(
            "skills_per_posting",
            F.col("distinct_skills").cast("double") / safe_count,
        )
        .withColumn(
            "companies_per_posting",
            F.col("distinct_companies").cast("double") / safe_count,
        )
    )


def add_high_demand_label(df: DataFrame) -> DataFrame:
    """Derive a per-position top-quantile ``high_demand`` binary label."""
    thresholds = (
        df.groupBy("search_position")
        .agg(
            F.expr(
                "percentile_approx(group_count, {q})".format(
                    q=HIGH_DEMAND_QUANTILE
                )
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


def random_split(df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """Random ``TRAIN_FRACTION`` / ``1 - TRAIN_FRACTION`` split with a fixed seed."""
    train, test = df.randomSplit(
        [TRAIN_FRACTION, 1.0 - TRAIN_FRACTION], seed=RANDOM_SEED
    )
    return train, test


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

    agg = aggregate_groups(raw)
    rows_before = agg.count()
    agg = filter_small_groups(agg, MIN_POSTINGS_PER_GROUP)
    rows_after = agg.count()
    print(
        f"[stage3.prepare] Small-group filter (group_count >= "
        f"{MIN_POSTINGS_PER_GROUP}): {rows_before} -> {rows_after} groups",
        flush=True,
    )
    if rows_after == 0:
        print(
            "[stage3.prepare] FATAL: no (country, position) groups left after "
            "filtering. Check the source enriched table.",
            flush=True,
        )
        spark.stop()
        sys.exit(1)

    agg = add_derived_features(agg)
    agg = add_high_demand_label(agg)
    agg.cache()

    train, test = random_split(agg)
    train_count = train.count()
    test_count = test.count()
    print(
        f"[stage3.prepare] Random split (seed={RANDOM_SEED}): "
        f"{train_count} train / {test_count} test groups",
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
        fh.write("aggregation=country_position\n")
        fh.write(f"min_postings_per_group={MIN_POSTINGS_PER_GROUP}\n")
        fh.write(f"high_demand_quantile={HIGH_DEMAND_QUANTILE}\n")
        fh.write(f"rows_before_filter={rows_before}\n")
        fh.write(f"rows_after_filter={rows_after}\n")
        fh.write(f"train_rows={train_count}\n")
        fh.write(f"test_rows={test_count}\n")
        fh.write(f"split_seed={RANDOM_SEED}\n")
        fh.write(f"train_path={train_path}\n")
        fh.write(f"test_path={test_path}\n")
    print(f"[stage3.prepare] Summary written to {summary_path}", flush=True)

    spark.stop()


if __name__ == "__main__":
    main()
