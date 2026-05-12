"""Stage 3.3 — Generate sample predictions for the trained classifiers.

Loads each of the best models (rf, gbt, svm, nb) saved by
``stage3_train_models.py`` from HDFS, applies them to:

* a small random sample drawn from the held-out test set, and
* a few hand-crafted (country, position) scenarios built with the
  median values of the derived numeric features from the test set,

then writes a single combined CSV of predictions to ``output/`` that
also includes a ``pred_ensemble`` column (soft-voted rf + gbt + svm).
The file is intentionally small so the grader can read it without
HDFS access.
"""

import argparse
from pathlib import Path
from typing import Iterable, List

from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType


DEFAULT_ML_BASE = "/user/team32/linkedin/ml"
MODEL_NAMES = ("rf", "gbt", "svm", "nb")

# Members of the soft-voting ensemble (keep in sync with stage3_train_models.py).
ENSEMBLE_MEMBERS = ("rf", "gbt", "svm")
ENSEMBLE_THRESHOLD = 0.5

# (country, position) pairs used to construct hand-crafted "what-if"
# scenarios. They are not necessarily in the test set — that is the
# whole point of having them.
CRAFTED_SCENARIOS = (
    ("united states", "software engineer"),
    ("united kingdom", "data scientist"),
    ("germany", "product manager"),
    ("india", "software engineer"),
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
    return parser.parse_args()


def build_spark() -> SparkSession:
    """Create the Spark session used by this stage."""
    return (
        SparkSession.builder
        .appName("team32-stage3-predict-samples")
        .getOrCreate()
    )


def median_feature_values(test: DataFrame) -> Row:
    """Return median values of the derived numeric features as defaults."""
    return test.agg(
        F.expr("percentile_approx(summary_coverage, 0.5)").alias("summary_coverage"),
        F.expr("percentile_approx(distinct_job_levels, 0.5)").alias("distinct_job_levels"),
        F.expr("percentile_approx(distinct_job_types, 0.5)").alias("distinct_job_types"),
        F.expr("percentile_approx(skills_per_posting, 0.5)").alias("skills_per_posting"),
        F.expr("percentile_approx(companies_per_posting, 0.5)").alias("companies_per_posting"),
        F.expr("percentile_approx(distinct_companies, 0.5)").alias("distinct_companies"),
        F.expr("percentile_approx(distinct_skills, 0.5)").alias("distinct_skills"),
        F.expr("percentile_approx(group_count, 0.5)").alias("group_count"),
    ).first()


def build_crafted_rows(
    scenarios: Iterable[tuple],
    defaults: Row,
    schema: StructType,
) -> List[tuple]:
    """Build crafted rows as tuples in ``schema`` field order.

    We avoid ``pyspark.sql.Row(**kwargs)`` because PySpark sorts kwargs
    alphabetically, which would misalign the values when the row is
    later interpreted under an explicit schema.
    """
    summary_coverage = float(defaults["summary_coverage"] or 0.0)
    distinct_job_levels = int(defaults["distinct_job_levels"] or 0)
    distinct_job_types = int(defaults["distinct_job_types"] or 0)
    skills_per_posting = float(defaults["skills_per_posting"] or 0.0)
    companies_per_posting = float(defaults["companies_per_posting"] or 0.0)
    distinct_companies = int(defaults["distinct_companies"] or 0)
    distinct_skills = int(defaults["distinct_skills"] or 0)
    group_count = int(defaults["group_count"] or 0)

    field_names = [f.name for f in schema.fields]
    rows: List[tuple] = []
    for country, position in scenarios:
        values = {
            "search_country": country,
            "search_position": position,
            "group_count": group_count,
            "distinct_companies": distinct_companies,
            "distinct_skills": distinct_skills,
            "distinct_job_levels": distinct_job_levels,
            "distinct_job_types": distinct_job_types,
            "summary_coverage": summary_coverage,
            "skills_per_posting": skills_per_posting,
            "companies_per_posting": companies_per_posting,
            "high_demand": 0,
        }
        rows.append(tuple(values.get(name) for name in field_names))
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

    defaults = median_feature_values(test)
    crafted_rows = build_crafted_rows(CRAFTED_SCENARIOS, defaults, test.schema)

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
        "search_country",
        "search_position",
        F.col("high_demand").alias("true_label"),
    )

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    merged = base
    prob_dfs = {}
    for name in MODEL_NAMES:
        model_path = f"{args.ml_base}/models/{name}"
        print(f"[stage3.predict] Loading model: {model_path}", flush=True)
        model = PipelineModel.load(model_path)
        preds = model.transform(combined)

        pred_col = preds.select(
            "row_id", F.col("prediction").cast("int").alias(f"pred_{name}")
        )
        merged = merged.join(pred_col, "row_id", "left")

        # Only ensemble members need to contribute a positive-class
        # probability column. NB is excluded for the same reason as in
        # stage3_train_models.py (uncalibrated outputs).
        if name in ENSEMBLE_MEMBERS:
            if "probability" in preds.columns:
                prob = (
                    preds.withColumn("_pa", vector_to_array("probability"))
                    .withColumn(f"prob_{name}", F.col("_pa")[1])
                    .select("row_id", f"prob_{name}")
                )
            else:
                prob = (
                    preds.withColumn("_ra", vector_to_array("rawPrediction"))
                    .withColumn(
                        f"prob_{name}",
                        F.expr("1.0 / (1.0 + exp(-_ra[1]))"),
                    )
                    .select("row_id", f"prob_{name}")
                )
            prob_dfs[name] = prob

    # Soft-voting ensemble: average the per-model positive-class
    # probabilities, threshold at ``ENSEMBLE_THRESHOLD``.
    ensemble_df = combined.select("row_id")
    for name in ENSEMBLE_MEMBERS:
        ensemble_df = ensemble_df.join(prob_dfs[name], "row_id")
    prob_cols = [F.col(f"prob_{n}") for n in ENSEMBLE_MEMBERS]
    ensemble_df = ensemble_df.withColumn(
        "pred_ensemble",
        F.when(
            sum(prob_cols) / float(len(ENSEMBLE_MEMBERS))
            >= F.lit(ENSEMBLE_THRESHOLD),
            F.lit(1),
        )
        .otherwise(F.lit(0))
        .cast("int"),
    ).select("row_id", "pred_ensemble")
    merged = merged.join(ensemble_df, "row_id", "left")

    out_path = out_dir / "stage3_sample_predictions.csv"
    print(f"[stage3.predict] Writing predictions -> {out_path}", flush=True)
    (
        merged.drop("row_id")
        .orderBy("source", "search_country", "search_position")
        .toPandas()
        .to_csv(out_path, index=False)
    )

    spark.stop()


if __name__ == "__main__":
    main()
