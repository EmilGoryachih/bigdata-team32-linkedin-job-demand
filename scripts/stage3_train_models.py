"""Stage 3.2 — Train, tune, and evaluate demand-prediction classifiers.

Reads the train/test Parquet datasets produced by
``stage3_prepare_dataset.py``, builds Spark ML pipelines for
RandomForest, LinearSVC, and Multinomial NaiveBayes, performs 3-fold
cross-validated hyperparameter search on each, evaluates the best
pipeline on the held-out test split, and persists the best models back
to HDFS.

Per-model metrics, best hyperparameters, and confusion matrices are
written to the local ``output/`` directory so the grader can read them
without HDFS access.
"""

from __future__ import annotations

import argparse
import csv
import math
from pathlib import Path
from typing import Dict, List, Tuple

from pyspark.ml import Pipeline
from pyspark.ml.classification import (
    LinearSVC,
    NaiveBayes,
    RandomForestClassifier,
)
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator,
)
from pyspark.ml.feature import (
    MinMaxScaler,
    OneHotEncoder,
    StandardScaler,
    StringIndexer,
    VectorAssembler,
)
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


DEFAULT_ML_BASE = "/user/team32/linkedin/ml"
LABEL_COL = "high_demand"

CATEGORICAL_COLS: Tuple[str, ...] = (
    "search_country",
    "search_city",
    "search_position",
)

# Numeric feature columns. ``group_count`` is deliberately excluded —
# the label is a deterministic function of it, so including it would be
# trivial leakage. The lag/rolling variants are safe because they only
# look at past time buckets.
NUMERIC_COLS: Tuple[str, ...] = (
    "year",
    "month",
    "week_of_year",
    "day_of_week",
    "quarter",
    "is_weekend",
    "lag_count_1",
    "lag_count_4",
    "rolling_mean_count_4",
    "lag_distinct_companies_1",
    "lag_distinct_skills_1",
)

CV_FOLDS = 3
CV_PARALLELISM = 2
RANDOM_SEED = 42


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ml-base", default=DEFAULT_ML_BASE)
    parser.add_argument("--output-dir", default="output")
    return parser.parse_args()


def build_spark() -> SparkSession:
    """Create the Spark session used by this stage."""
    return (
        SparkSession.builder
        .appName("team32-stage3-train-models")
        .getOrCreate()
    )


def build_feature_stages(scaler: str | None) -> List:
    """Build the shared preprocessing stages.

    ``scaler`` selects the optional scaling step appended to the
    assembled feature vector:

    * ``None``      — no scaling, classifier reads ``raw_features``.
    * ``"standard"``— ``StandardScaler`` (mean=False to keep one-hot
      vectors sparse). Output column ``features``.
    * ``"minmax"``  — ``MinMaxScaler``; output column ``features``.
      Used for NaiveBayes which requires non-negative inputs.
    """
    stages: List = []
    encoded_cols: List[str] = []
    for col_name in CATEGORICAL_COLS:
        indexer = StringIndexer(
            inputCol=col_name,
            outputCol=f"{col_name}_idx",
            handleInvalid="keep",
        )
        encoder = OneHotEncoder(
            inputCol=f"{col_name}_idx",
            outputCol=f"{col_name}_oh",
            handleInvalid="keep",
        )
        stages.extend([indexer, encoder])
        encoded_cols.append(f"{col_name}_oh")

    assembler = VectorAssembler(
        inputCols=list(NUMERIC_COLS) + encoded_cols,
        outputCol="raw_features",
        handleInvalid="keep",
    )
    stages.append(assembler)

    if scaler == "standard":
        stages.append(
            StandardScaler(
                inputCol="raw_features",
                outputCol="features",
                withMean=False,
                withStd=True,
            )
        )
    elif scaler == "minmax":
        stages.append(
            MinMaxScaler(inputCol="raw_features", outputCol="features")
        )
    return stages


def make_rf() -> Tuple[Pipeline, list, RandomForestClassifier]:
    """Build the Random Forest pipeline and tuning grid."""
    stages = build_feature_stages(scaler=None)
    rf = RandomForestClassifier(
        labelCol=LABEL_COL,
        featuresCol="raw_features",
        seed=RANDOM_SEED,
    )
    grid = (
        ParamGridBuilder()
        .addGrid(rf.numTrees, [50, 100, 200])
        .addGrid(rf.maxDepth, [5, 10, 15])
        .build()
    )
    return Pipeline(stages=stages + [rf]), grid, rf


def make_svm() -> Tuple[Pipeline, list, LinearSVC]:
    """Build the Linear SVC pipeline and tuning grid."""
    stages = build_feature_stages(scaler="standard")
    svm = LinearSVC(labelCol=LABEL_COL, featuresCol="features")
    grid = (
        ParamGridBuilder()
        .addGrid(svm.regParam, [0.001, 0.01, 0.1])
        .addGrid(svm.maxIter, [50, 100])
        .build()
    )
    return Pipeline(stages=stages + [svm]), grid, svm


def make_nb() -> Tuple[Pipeline, list, NaiveBayes]:
    """Build the multinomial Naive Bayes pipeline and tuning grid."""
    stages = build_feature_stages(scaler="minmax")
    nb = NaiveBayes(
        labelCol=LABEL_COL,
        featuresCol="features",
        modelType="multinomial",
    )
    grid = (
        ParamGridBuilder()
        .addGrid(nb.smoothing, [0.5, 1.0, 2.0])
        .build()
    )
    return Pipeline(stages=stages + [nb]), grid, nb


def evaluate(predictions: DataFrame) -> Tuple[float, float, float]:
    """Compute accuracy, F1, and AUC on the prediction frame."""
    acc = MulticlassClassificationEvaluator(
        labelCol=LABEL_COL,
        predictionCol="prediction",
        metricName="accuracy",
    ).evaluate(predictions)
    f1 = MulticlassClassificationEvaluator(
        labelCol=LABEL_COL,
        predictionCol="prediction",
        metricName="f1",
    ).evaluate(predictions)

    # rawPrediction is produced by RF and LinearSVC but not by NaiveBayes
    # (which exposes probability instead). Fall back to NaN when AUC
    # cannot be computed.
    auc: float
    if "rawPrediction" in predictions.columns:
        auc = BinaryClassificationEvaluator(
            labelCol=LABEL_COL,
            rawPredictionCol="rawPrediction",
            metricName="areaUnderROC",
        ).evaluate(predictions)
    else:
        auc = math.nan
    return acc, f1, auc


def confusion_pdf(predictions: DataFrame):
    """Return a pandas dataframe with the confusion matrix counts."""
    return (
        predictions.groupBy(LABEL_COL, "prediction")
        .count()
        .orderBy(LABEL_COL, "prediction")
        .toPandas()
    )


def extract_best_params(cv_model, classifier) -> Dict[str, object]:
    """Pull only the classifier's tuned parameters out of the best model."""
    best_stage = cv_model.bestModel.stages[-1]
    param_map = best_stage.extractParamMap()
    return {
        param.name: value
        for param, value in param_map.items()
        if param.parent == classifier.uid
    }


def train_one(
    name: str,
    pipeline: Pipeline,
    grid: list,
    classifier,
    train: DataFrame,
    test: DataFrame,
    ml_base: str,
    out_dir: Path,
    metrics_writer,
) -> None:
    """Run CV on one estimator, evaluate, persist artifacts."""
    print(f"[stage3.train] === {name.upper()}: cross-validated tuning ===", flush=True)

    evaluator = MulticlassClassificationEvaluator(
        labelCol=LABEL_COL, predictionCol="prediction", metricName="f1"
    )
    cv = CrossValidator(
        estimator=pipeline,
        estimatorParamMaps=grid,
        evaluator=evaluator,
        numFolds=CV_FOLDS,
        parallelism=CV_PARALLELISM,
        seed=RANDOM_SEED,
    )
    cv_model = cv.fit(train)
    predictions = cv_model.transform(test)

    acc, f1, auc = evaluate(predictions)
    print(
        f"[stage3.train] {name}: accuracy={acc:.4f}  f1={f1:.4f}  auc={auc:.4f}",
        flush=True,
    )

    model_path = f"{ml_base}/models/{name}"
    print(f"[stage3.train] {name}: saving best model -> {model_path}", flush=True)
    cv_model.bestModel.write().overwrite().save(model_path)

    metrics_writer.writerow([name, f"{acc:.6f}", f"{f1:.6f}", f"{auc:.6f}"])

    params = extract_best_params(cv_model, classifier)
    params_path = out_dir / f"stage3_{name}_best_params.txt"
    with params_path.open("w", encoding="utf-8") as fh:
        for key, value in sorted(params.items()):
            fh.write(f"{key}={value}\n")

    confusion = confusion_pdf(predictions)
    confusion.to_csv(out_dir / f"stage3_{name}_confusion.csv", index=False)


def main() -> None:
    """Entry point for ``spark-submit``."""
    args = parse_args()
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    train_path = f"{args.ml_base}/dataset_train"
    test_path = f"{args.ml_base}/dataset_test"
    print(f"[stage3.train] Reading train: {train_path}", flush=True)
    print(f"[stage3.train] Reading test:  {test_path}", flush=True)
    train = spark.read.parquet(train_path).cache()
    test = spark.read.parquet(test_path).cache()
    print(
        f"[stage3.train] Train rows: {train.count()}, test rows: {test.count()}",
        flush=True,
    )

    # Quick label balance sanity check.
    print("[stage3.train] Train label distribution:", flush=True)
    train.groupBy(LABEL_COL).count().show()

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    metrics_path = out_dir / "stage3_model_metrics.csv"

    with metrics_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["model", "accuracy", "f1", "auc"])
        for name, builder in (("rf", make_rf), ("svm", make_svm), ("nb", make_nb)):
            pipeline, grid, classifier = builder()
            train_one(
                name=name,
                pipeline=pipeline,
                grid=grid,
                classifier=classifier,
                train=train,
                test=test,
                ml_base=args.ml_base,
                out_dir=out_dir,
                metrics_writer=writer,
            )

    print(f"[stage3.train] Metrics summary -> {metrics_path}", flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
