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

import argparse
import csv
import math
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import (
    GBTClassifier,
    LinearSVC,
    NaiveBayes,
    RandomForestClassifier,
)
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator,
)
from pyspark.ml.feature import (
    OneHotEncoder,
    StandardScaler,
    StringIndexer,
    VectorAssembler,
)
from pyspark.ml.functions import vector_to_array
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


DEFAULT_ML_BASE = "/user/team32/linkedin/ml"
LABEL_COL = "high_demand"

CATEGORICAL_COLS: Tuple[str, ...] = (
    "search_country",
    "search_position",
)

# Numeric feature columns. ``group_count``, ``distinct_companies``,
# and ``distinct_skills`` are deliberately excluded — the label is a
# deterministic function of ``group_count``, and the raw distinct
# counts are bounded by it (their upper bound is group_count, so they
# leak the label). We keep the per-posting ratio variants instead.
NUMERIC_COLS: Tuple[str, ...] = (
    "summary_coverage",
    "distinct_job_levels",
    "distinct_job_types",
    "skills_per_posting",
    "companies_per_posting",
)

CV_FOLDS = 3
# Up from 2 to 4: with 27-combination grids the wall time grows
# proportionally, so we let Spark schedule more candidate fits in
# parallel inside each fold.
CV_PARALLELISM = 4
RANDOM_SEED = 42

# Soft-voting ensemble configuration. NB is excluded because its raw /
# probability outputs are not well-calibrated for this dataset, so it
# would only drag the averaged score down.
ENSEMBLE_NAME = "ensemble"
ENSEMBLE_MEMBERS: Tuple[str, ...] = ("rf", "gbt", "svm")
ENSEMBLE_THRESHOLD = 0.5


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


def build_feature_stages(scaler: Optional[str]) -> List:
    """Build the shared preprocessing stages.

    ``scaler`` selects the optional scaling step appended to the
    assembled feature vector:

    * ``None``      — no scaling, classifier reads ``raw_features``.
      Used by RF, GBT, and Gaussian NB (none of them care about
      feature magnitudes).
    * ``"standard"``— ``StandardScaler`` (mean=False to keep one-hot
      vectors sparse and non-negative). Output column ``features``.
      Used by LinearSVC.
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
    return stages


def make_rf() -> Tuple[Pipeline, list, RandomForestClassifier]:
    """Build the Random Forest pipeline and tuning grid.

    Grid follows the course rubric: 3 hyperparameters x 3 values =
    27 combinations, with one algorithm hyperparameter
    (``numTrees`` — size of the ensemble) and two model
    hyperparameters (``maxDepth``, ``minInstancesPerNode``).
    """
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
        .addGrid(rf.minInstancesPerNode, [1, 5, 10])
        .build()
    )
    return Pipeline(stages=stages + [rf]), grid, rf


def make_gbt() -> Tuple[Pipeline, list, GBTClassifier]:
    """Build the Gradient Boosted Trees pipeline and tuning grid.

    Grid: 3 x 3 x 3 = 27 combinations. ``maxIter`` is excluded
    because the rubric forbids iteration counts as hyperparameters;
    we use ``stepSize`` (learning rate) as the algorithm
    hyperparameter and ``maxDepth`` + ``minInstancesPerNode`` as the
    model ones.
    """
    stages = build_feature_stages(scaler=None)
    gbt = GBTClassifier(
        labelCol=LABEL_COL,
        featuresCol="raw_features",
        seed=RANDOM_SEED,
    )
    grid = (
        ParamGridBuilder()
        .addGrid(gbt.stepSize, [0.05, 0.1, 0.2])
        .addGrid(gbt.maxDepth, [3, 5, 8])
        .addGrid(gbt.minInstancesPerNode, [1, 5, 10])
        .build()
    )
    return Pipeline(stages=stages + [gbt]), grid, gbt


def make_svm() -> Tuple[Pipeline, list, LinearSVC]:
    """Build the Linear SVC pipeline and tuning grid.

    Grid: 3 x 3 x 3 = 27 combinations. ``maxIter`` is excluded
    (rubric). ``aggregationDepth`` is the algorithm hyperparameter
    (it controls how distributed gradient aggregations are reduced),
    ``regParam`` and ``threshold`` are the two model hyperparameters
    (L2 strength and decision-margin threshold respectively).
    """
    stages = build_feature_stages(scaler="standard")
    svm = LinearSVC(labelCol=LABEL_COL, featuresCol="features")
    grid = (
        ParamGridBuilder()
        .addGrid(svm.regParam, [0.001, 0.01, 0.1])
        .addGrid(svm.threshold, [-0.2, 0.0, 0.2])
        .addGrid(svm.aggregationDepth, [2, 3, 4])
        .build()
    )
    return Pipeline(stages=stages + [svm]), grid, svm


def make_nb() -> Tuple[Pipeline, list, NaiveBayes]:
    """Build the Naive Bayes pipeline and tuning grid.

    Grid: 3 x 3 x 3 = 27 combinations. ``modelType`` is the
    algorithm hyperparameter (we try gaussian / multinomial /
    complement — bernoulli is excluded because it requires binary
    features). ``smoothing`` and ``thresholds`` are model
    hyperparameters: the first is variance / Laplace smoothing, the
    second shifts the decision boundary which is useful given our
    13/87 class imbalance.

    All numeric features and one-hot encodings are non-negative, so
    multinomial and complement variants run without an extra scaler.
    """
    stages = build_feature_stages(scaler=None)
    nb = NaiveBayes(
        labelCol=LABEL_COL,
        featuresCol="raw_features",
        modelType="gaussian",
    )
    grid = (
        ParamGridBuilder()
        .addGrid(nb.smoothing, [0.1, 1.0, 5.0])
        .addGrid(nb.modelType, ["gaussian", "multinomial", "complement"])
        .addGrid(nb.thresholds, [[0.3, 0.7], [0.5, 0.5], [0.7, 0.3]])
        .build()
    )
    return Pipeline(stages=stages + [nb]), grid, nb


def evaluate(predictions: DataFrame) -> Tuple[float, float, float, float]:
    """Compute accuracy, F1, AUC-ROC, and AUC-PR on the predictions.

    Both AUC metrics are reported because the course rubric requires
    "Area Under ROC and Area Under PR for binary classification".
    AUC-PR is especially informative on our 13/87 imbalanced label.
    """
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

    # rawPrediction is produced by RF, GBT, and LinearSVC; Spark's
    # NaiveBayes also exposes it. Fall back to NaN in the unlikely
    # event it is missing so the loop still completes.
    if "rawPrediction" in predictions.columns:
        auc_roc = BinaryClassificationEvaluator(
            labelCol=LABEL_COL,
            rawPredictionCol="rawPrediction",
            metricName="areaUnderROC",
        ).evaluate(predictions)
        auc_pr = BinaryClassificationEvaluator(
            labelCol=LABEL_COL,
            rawPredictionCol="rawPrediction",
            metricName="areaUnderPR",
        ).evaluate(predictions)
    else:
        auc_roc = math.nan
        auc_pr = math.nan
    return acc, f1, auc_roc, auc_pr


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


def feature_names_from_pipeline(model: PipelineModel) -> List[str]:
    """Recover the per-slot feature names of the assembled feature vector.

    Walks the fitted pipeline, picks up the categorical labels from each
    StringIndexerModel and the assembled input order from the
    VectorAssembler, and reconstructs a human-readable name for every
    column of ``featureImportances``. Best-effort — if the pipeline has
    an unexpected shape the function falls back to generic names.
    """
    idx_labels: Dict[str, List[str]] = {}  # indexer output col -> labels
    oh_input: Dict[str, str] = {}          # oh output col -> indexer output col
    assembler_inputs: List[str] = []

    for stage in model.stages:
        cls_name = type(stage).__name__
        if cls_name == "StringIndexerModel":
            try:
                idx_labels[stage.getOutputCol()] = list(stage.labels)
            except Exception:  # pylint: disable=broad-except
                pass
        elif cls_name == "OneHotEncoderModel":
            try:
                oh_input[stage.getOutputCol()] = stage.getInputCol()
            except Exception:  # pylint: disable=broad-except
                pass
        elif cls_name == "VectorAssembler":
            try:
                assembler_inputs = list(stage.getInputCols())
            except Exception:  # pylint: disable=broad-except
                pass

    names: List[str] = []
    for col in assembler_inputs:
        if col in oh_input:
            base = col[:-3] if col.endswith("_oh") else col
            for label in idx_labels.get(oh_input[col], []):
                names.append(f"{base}={label}")
        else:
            names.append(col)
    return names


def write_feature_importance(
    model: PipelineModel, model_name: str, out_dir: Path
) -> bool:
    """Persist sorted feature importances for tree-based classifiers.

    Returns ``True`` when a file was written (RF, GBT) and ``False``
    when the classifier does not expose ``featureImportances`` (SVM,
    Gaussian NB).
    """
    classifier = model.stages[-1]
    if not hasattr(classifier, "featureImportances"):
        return False

    importances = classifier.featureImportances.toArray()
    names = feature_names_from_pipeline(model)

    # Reconcile lengths defensively — the heuristic name extraction can
    # disagree with the actual vector length under unusual encoder
    # configurations.
    n_features = len(importances)
    if len(names) < n_features:
        names = names + [
            f"feature_{i}" for i in range(len(names), n_features)
        ]
    elif len(names) > n_features:
        names = names[:n_features]

    pairs = sorted(
        zip(names, importances),
        key=lambda pair: float(pair[1]),
        reverse=True,
    )
    out_path = out_dir / f"stage3_{model_name}_feature_importance.csv"
    with out_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["feature", "importance"])
        for feature, importance in pairs:
            writer.writerow([feature, f"{float(importance):.6f}"])
    return True


def _format_param_value(value: object) -> str:
    """Render a param value for human-readable CSV/TXT output.

    NaiveBayes' ``thresholds`` parameter is an ``Array[Double]`` —
    rendering it as the default Python ``str(list)`` would inject
    commas that break CSV columns. We collapse list values into a
    pipe-separated string instead so each row stays one cell wide.
    """
    if isinstance(value, (list, tuple)):
        return "|".join(str(v) for v in value)
    return str(value)


def write_cv_grid(
    cv_model,
    grid: list,
    classifier,
    model_name: str,
    out_dir: Path,
) -> None:
    """Write the full CV parameter grid with mean F1 per combination.

    ``cv_model.avgMetrics`` gives the mean validation F1 across the K
    folds for every entry in ``grid``, in the same order. The output
    file makes the tuning process auditable: which combinations were
    tried and how each one scored.
    """
    scores = list(cv_model.avgMetrics)
    rows: List[Tuple[Dict[str, object], float]] = []
    for combo, score in zip(grid, scores):
        params = {
            param.name: value
            for param, value in combo.items()
            if param.parent == classifier.uid
        }
        rows.append((params, float(score)))
    rows.sort(key=lambda row: row[1], reverse=True)

    param_keys = sorted({key for params, _ in rows for key in params})
    out_path = out_dir / f"stage3_{model_name}_cv_grid.csv"
    with out_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(param_keys + ["mean_f1"])
        for params, score in rows:
            writer.writerow(
                [_format_param_value(params.get(key, "")) for key in param_keys]
                + [f"{score:.6f}"]
            )


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
) -> PipelineModel:
    """Run CV on one estimator, evaluate, persist artifacts, return best model."""
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

    acc, f1, auc_roc, auc_pr = evaluate(predictions)
    print(
        f"[stage3.train] {name}: accuracy={acc:.4f}  f1={f1:.4f}  "
        f"auc_roc={auc_roc:.4f}  auc_pr={auc_pr:.4f}",
        flush=True,
    )

    model_path = f"{ml_base}/models/{name}"
    print(f"[stage3.train] {name}: saving best model -> {model_path}", flush=True)
    cv_model.bestModel.write().overwrite().save(model_path)

    metrics_writer.writerow(
        [name, f"{acc:.6f}", f"{f1:.6f}", f"{auc_roc:.6f}", f"{auc_pr:.6f}"]
    )

    params = extract_best_params(cv_model, classifier)
    params_path = out_dir / f"stage3_{name}_best_params.txt"
    with params_path.open("w", encoding="utf-8") as fh:
        for key, value in sorted(params.items()):
            fh.write(f"{key}={value}\n")

    # Full CV grid (per-combination mean F1) and tree-based feature
    # importance. The latter is a no-op for SVM / Gaussian NB.
    write_cv_grid(cv_model, grid, classifier, name, out_dir)
    write_feature_importance(cv_model.bestModel, name, out_dir)

    confusion = confusion_pdf(predictions)
    confusion.to_csv(out_dir / f"stage3_{name}_confusion.csv", index=False)

    return cv_model.bestModel


def _positive_class_prob(pred: DataFrame, model_name: str) -> DataFrame:
    """Return a ``(row_id, prob_<model_name>)`` frame with ``P(class=1)``.

    Tree models and Naive Bayes expose a ``probability`` vector; LinearSVC
    only exposes ``rawPrediction`` (signed margins), so we squash the
    positive-class margin through a sigmoid to get a comparable scalar
    in ``[0, 1]``.
    """
    out_col = f"prob_{model_name}"
    if "probability" in pred.columns:
        return (
            pred.withColumn("_pa", vector_to_array("probability"))
            .withColumn(out_col, F.col("_pa")[1])
            .select("row_id", out_col)
        )
    return (
        pred.withColumn("_ra", vector_to_array("rawPrediction"))
        .withColumn(out_col, F.expr("1.0 / (1.0 + exp(-_ra[1]))"))
        .select("row_id", out_col)
    )


def build_ensemble(
    trained_models: Dict[str, PipelineModel],
    test: DataFrame,
    member_names: Tuple[str, ...],
) -> DataFrame:
    """Soft-vote ensemble: average ``P(class=1)`` across the members."""
    test_with_id = test.withColumn(
        "row_id", F.monotonically_increasing_id()
    ).cache()
    # Force IDs to materialise so the same row_id is seen by every model
    # transform below (monotonically_increasing_id is per-partition; once
    # the DataFrame is cached the values are frozen).
    test_with_id.count()

    ensemble = test_with_id.select("row_id", LABEL_COL)
    for name in member_names:
        pred = trained_models[name].transform(test_with_id)
        ensemble = ensemble.join(_positive_class_prob(pred, name), "row_id")

    prob_cols = [F.col(f"prob_{n}") for n in member_names]
    return (
        ensemble.withColumn(
            "prob_ensemble",
            sum(prob_cols) / float(len(member_names)),
        )
        .withColumn(
            "prediction",
            F.when(
                F.col("prob_ensemble") >= F.lit(ENSEMBLE_THRESHOLD), F.lit(1.0)
            ).otherwise(F.lit(0.0)),
        )
    )


def evaluate_ensemble(predictions: DataFrame) -> Tuple[float, float, float, float]:
    """Accuracy / F1 / AUC-ROC / AUC-PR for the soft-voted ensemble."""
    acc = MulticlassClassificationEvaluator(
        labelCol=LABEL_COL, predictionCol="prediction", metricName="accuracy"
    ).evaluate(predictions)
    f1 = MulticlassClassificationEvaluator(
        labelCol=LABEL_COL, predictionCol="prediction", metricName="f1"
    ).evaluate(predictions)
    auc_roc = BinaryClassificationEvaluator(
        labelCol=LABEL_COL,
        rawPredictionCol="prob_ensemble",
        metricName="areaUnderROC",
    ).evaluate(predictions)
    auc_pr = BinaryClassificationEvaluator(
        labelCol=LABEL_COL,
        rawPredictionCol="prob_ensemble",
        metricName="areaUnderPR",
    ).evaluate(predictions)
    return acc, f1, auc_roc, auc_pr


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

    trained_models: Dict[str, PipelineModel] = {}
    with metrics_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["model", "accuracy", "f1", "auc_roc", "auc_pr"])
        for name, builder in (
            ("rf", make_rf),
            ("gbt", make_gbt),
            ("svm", make_svm),
            ("nb", make_nb),
        ):
            pipeline, grid, classifier = builder()
            trained_models[name] = train_one(
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

        # Soft-voting ensemble of the strong non-NB models.
        print(
            f"[stage3.train] === {ENSEMBLE_NAME.upper()} "
            f"(soft voting of {', '.join(ENSEMBLE_MEMBERS)}) ===",
            flush=True,
        )
        ensemble_pred = build_ensemble(trained_models, test, ENSEMBLE_MEMBERS)
        acc, f1, auc_roc, auc_pr = evaluate_ensemble(ensemble_pred)
        print(
            f"[stage3.train] {ENSEMBLE_NAME}: accuracy={acc:.4f}  "
            f"f1={f1:.4f}  auc_roc={auc_roc:.4f}  auc_pr={auc_pr:.4f}",
            flush=True,
        )
        writer.writerow(
            [
                ENSEMBLE_NAME,
                f"{acc:.6f}",
                f"{f1:.6f}",
                f"{auc_roc:.6f}",
                f"{auc_pr:.6f}",
            ]
        )
        confusion_pdf(ensemble_pred).to_csv(
            out_dir / f"stage3_{ENSEMBLE_NAME}_confusion.csv", index=False
        )
        info_path = out_dir / f"stage3_{ENSEMBLE_NAME}_info.txt"
        with info_path.open("w", encoding="utf-8") as fh_info:
            fh_info.write(f"members={','.join(ENSEMBLE_MEMBERS)}\n")
            fh_info.write(
                "strategy=soft_voting_mean_of_positive_class_probability\n"
            )
            fh_info.write(f"threshold={ENSEMBLE_THRESHOLD}\n")
            fh_info.write(
                "note=NB excluded; its raw/probability outputs are not "
                "well-calibrated on this dataset.\n"
            )

    print(f"[stage3.train] Metrics summary -> {metrics_path}", flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
