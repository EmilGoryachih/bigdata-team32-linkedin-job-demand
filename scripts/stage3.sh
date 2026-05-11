#!/usr/bin/env bash
set -euo pipefail

# Stage 3: Spark ML pipeline for temporal demand prediction.
# Runs three spark-submit jobs in sequence:
#   3.1  prepare aggregated ML dataset on HDFS
#   3.2  train RF / SVM / NB with 3-fold CV, save best models to HDFS
#   3.3  generate sample predictions to output/

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [ -f "$PROJECT_ROOT/.env" ]; then
  set -a
  # shellcheck disable=SC1091
  source "$PROJECT_ROOT/.env"
  set +a
fi

: "${HDFS_BASE:=/user/team32/linkedin}"
: "${HIVE_DB:=team32_linkedin}"
: "${SPARK_MASTER:=yarn}"
: "${SPARK_DEPLOY_MODE:=client}"

ML_BASE="${HDFS_BASE}/ml"

mkdir -p "$PROJECT_ROOT/output"
mkdir -p "$PROJECT_ROOT/models"

SPARK_SUBMIT_ARGS=(
  --master "$SPARK_MASTER"
  --deploy-mode "$SPARK_DEPLOY_MODE"
  --conf spark.sql.session.timeZone=UTC
)

run_spark() {
  local script="$1"
  shift
  echo ">>> spark-submit $script $*"
  spark-submit "${SPARK_SUBMIT_ARGS[@]}" "$script" "$@"
}

echo "Stage 3.1 - Prepare ML dataset"
run_spark "$PROJECT_ROOT/scripts/stage3_prepare_dataset.py" \
  --enriched-path "$HDFS_BASE/enriched/linkedin_jobs_enriched" \
  --ml-base "$ML_BASE" \
  --output-dir "$PROJECT_ROOT/output"

echo "Stage 3.2 - Train, tune, and evaluate models"
run_spark "$PROJECT_ROOT/scripts/stage3_train_models.py" \
  --ml-base "$ML_BASE" \
  --output-dir "$PROJECT_ROOT/output"

echo "Stage 3.3 - Generate sample predictions"
run_spark "$PROJECT_ROOT/scripts/stage3_predict_samples.py" \
  --ml-base "$ML_BASE" \
  --output-dir "$PROJECT_ROOT/output"

echo "Stage 3 completed."
