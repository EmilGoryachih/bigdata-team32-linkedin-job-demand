"""Stage 3.3 — Generate sample predictions for the trained classifiers.

Loads each of the best models (rf, svm, nb) saved by
``stage3_train_models.py`` from HDFS, applies them to:

* a small random sample drawn from the held-out test set, and
* a few hand-crafted future-period scenarios (built with median lag
  values so the inputs are realistic),

then writes a single combined CSV of predictions to ``output/``. The
file is intentionally small so the grader can read it without HDFS.
"""

from __future__ import annotations

import argparse
from datetime import date, timedelta
from pathlib import Path
from typing import Iterable, List

from pyspark.ml import PipelineModel
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F


DEFAULT_ML_BASE = "/user/team32/linkedin/ml"
MODEL_NAMES = ("rf", "svm", "nb")

CRAFTED_SCENARIOS = (
    ("united states", "new york", "software engineer"),
    ("united kingdom", "london", "data scientist"),
    ("germany", "berlin", "product manager"),
    ("india", "bangalore", "software engineer"),
)


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ml-base", default=DEFAULT_ML_BASE)
    parser.add_argument("--output-dir", default="output")
    parser.add_argument(
        "--n-sample",
        type=int,
        default=20,
        help="Number of held-out rows to sample from the test set.",
    )
    parser.add_argument(
        "--future-offset-days",
        type=int,
        default=30,
        help="How far in the future the crafted scenarios should land.",
    )
    return parser.parse_args()


def build_spark() -> SparkSession:
    """Create the Spark session used by this stage."""
    return (
        SparkSession.builder
        .appName("team32-stage3-predict-samples")
        .getOrCreate()
    )


def median_lag_values(test: DataFrame) -> Row:
    """Return median lag/rolling values from the test set as defaults."""
    return test.agg(
        F.expr("percentile_approx(lag_count_1, 0.5)").alias("lag_count_1"),
        F.expr("percentile_approx(lag_count_4, 0.5)").alias("lag_count_4"),
        F.expr("percentile_approx(rolling_mean_count_4, 0.5)").alias(
            "rolling_mean_count_4"
        ),
        F.expr("percentile_approx(lag_distinct_companies_1, 0.5)").alias(
            "lag_distinct_companies_1"
        ),
        F.expr("percentile_approx(lag_distinct_skills_1, 0.5)").alias(
            "lag_distinct_skills_1"
        ),
    ).first()


def build_crafted_rows(
    scenarios: Iterable[tuple],
    future_day: date,
    defaults: Row,
) -> List[Row]:
    """Build a list of crafted Row objects for future-period scenarios."""
    rows: List[Row] = []
    iso_week = future_day.isocalendar()[1]
    iso_weekday = future_day.isoweekday()  # 1=Monday..7=Sunday
    # Spark's ``dayofweek`` returns 1=Sunday..7=Saturday, so adjust.
    spark_dow = (iso_weekday % 7) + 1
    quarter = (future_day.month - 1) // 3 + 1
    is_weekend = 1 if iso_weekday in (6, 7) else 0

    for country, city, position in scenarios:
        rows.append(
            Row(
                time_bucket=future_day,
                search_country=country,
                search_city=city,
                search_position=position,
                group_count=0,
                distinct_companies=0,
                distinct_skills=0,
                summary_coverage=0.0,
                year=future_day.year,
                month=future_day.month,
                week_of_year=iso_week,
                day_of_week=spark_dow,
                quarter=quarter,
                is_weekend=is_weekend,
                lag_count_1=float(defaults["lag_count_1"] or 0),
                lag_count_4=float(defaults["lag_count_4"] or 0),
                rolling_mean_count_4=float(defaults["rolling_mean_count_4"] or 0),
                lag_distinct_companies_1=float(defaults["lag_distinct_companies_1"] or 0),
                lag_distinct_skills_1=float(defaults["lag_distinct_skills_1"] or 0),
                high_demand=0,
            )
        )
    return rows


def main() -> None:
    """Entry point for ``spark-submit``."""
    args = parse_args()
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    test_path = f"{args.ml_base}/dataset_test"
    print(f"[stage3.predict] Reading test: {test_path}", flush=True)
    test = spark.read.parquet(test_path).cache()

    held_out = (
        test.orderBy(F.rand(seed=42)).limit(args.n_sample).withColumn(
            "source", F.lit("held_out")
        )
    )

    future_day = date.today() + timedelta(days=args.future_offset_days)
    defaults = median_lag_values(test)
    crafted_rows = build_crafted_rows(CRAFTED_SCENARIOS, future_day, defaults)

    # Build the crafted frame using the same schema as the test set so
    # ``unionByName`` works. ``source`` is added afterwards.
    crafted = (
        spark.createDataFrame(crafted_rows, schema=test.schema).withColumn(
            "source", F.lit("crafted")
        )
    )

    combined = held_out.unionByName(crafted).withColumn(
        "row_id", F.monotonically_increasing_id()
    ).cache()

    base = combined.select(
        "row_id",
        "source",
        "time_bucket",
        "search_country",
        "search_city",
        "search_position",
        F.col("high_demand").alias("true_label"),
    )

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    merged = base
    for name in MODEL_NAMES:
        model_path = f"{args.ml_base}/models/{name}"
        print(f"[stage3.predict] Loading model: {model_path}", flush=True)
        model = PipelineModel.load(model_path)
        preds = model.transform(combined).select(
            "row_id", F.col("prediction").cast("int").alias(f"pred_{name}")
        )
        merged = merged.join(preds, "row_id", "left")

    out_path = out_dir / "stage3_sample_predictions.csv"
    print(f"[stage3.predict] Writing predictions -> {out_path}", flush=True)
    (
        merged.drop("row_id")
        .orderBy("source", "time_bucket")
        .toPandas()
        .to_csv(out_path, index=False)
    )

    spark.stop()


if __name__ == "__main__":
    main()
