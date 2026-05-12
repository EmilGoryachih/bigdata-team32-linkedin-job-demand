#!/bin/bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [ -f "$PROJECT_ROOT/.env" ]; then
  set -a
  # shellcheck disable=SC1091
  source "$PROJECT_ROOT/.env"
  set +a
fi

: "${HDFS_BASE:=/user/team32/linkedin}"
: "${SPARK_MASTER:=yarn}"
: "${SPARK_DEPLOY_MODE:=client}"

echo "Stage 2: Hive raw, Parquet and enriched tables"
bash "$PROJECT_ROOT/scripts/stage2_hive_tables.sh"

echo "Stage 2: HiveQL dashboard analytics tables"
bash "$PROJECT_ROOT/scripts/stage2_analytics.sh"

echo "Stage 2: Spark SQL EDA analytics"
spark-submit \
  --master "$SPARK_MASTER" \
  --deploy-mode "$SPARK_DEPLOY_MODE" \
  --conf spark.sql.session.timeZone=UTC \
  "$PROJECT_ROOT/scripts/stage2_spark_eda.py" \
  --enriched-path "$HDFS_BASE/enriched/linkedin_jobs_enriched" \
  --spark-analytics-path "$HDFS_BASE/analytics_spark" \
  --output-dir "$PROJECT_ROOT/output"

echo "Stage 2 completed."
