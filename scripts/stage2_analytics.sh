#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [ -f "$PROJECT_ROOT/.env" ]; then
  set -a
  # shellcheck disable=SC1091
  source "$PROJECT_ROOT/.env"
  set +a
else
  echo ".env file not found. Copy .env.example to .env and configure it."
  exit 1
fi

: "${HDFS_BASE:=/user/team32/linkedin}"
: "${HIVE_DB:=team32_linkedin}"
: "${BEELINE_URL:=jdbc:hive2://hadoop-03.uni.innopolis.ru:10001/default}"
: "${HIVE_USER:=${DB_USER:-team32}}"
: "${HIVE_PASSWORD:=${DB_PASSWORD:-}}"

if [ -z "${HIVE_PASSWORD:-}" ]; then
  echo "ERROR: HIVE_PASSWORD is not set"
  exit 1
fi

mkdir -p "$PROJECT_ROOT/output"

run_beeline() {
  beeline \
    -u "$BEELINE_URL" \
    -n "$HIVE_USER" \
    -p "$HIVE_PASSWORD" \
    --hiveconf HIVE_DB="$HIVE_DB" \
    --hiveconf HDFS_BASE="$HDFS_BASE" \
    "$@"
}

export_csv() {
  local query="$1"
  local output_file="$2"

  run_beeline \
    --showHeader=true \
    --outputformat=csv2 \
    -e "USE $HIVE_DB; $query" > "$PROJECT_ROOT/output/$output_file"
}

echo "Cleaning old Hive analytics HDFS directory..."
hdfs dfs -rm -r -skipTrash -f "$HDFS_BASE/analytics"
hdfs dfs -mkdir -p "$HDFS_BASE/analytics"

echo "Creating Hive dashboard analytics tables..."
run_beeline -f "$PROJECT_ROOT/sql/hive/05_analytics_queries.hql"

echo "Exporting small Hive analytics CSV summaries..."
export_csv \
  "SELECT metric_name, metric_value FROM analytics_data_characteristics ORDER BY metric_name;" \
  "analytics_data_characteristics.csv"

export_csv \
  "SELECT search_position, posting_count, company_count, country_count FROM analytics_top_positions_global ORDER BY posting_count DESC LIMIT 50;" \
  "analytics_top_positions_global.csv"

export_csv \
  "SELECT skill, skill_mentions, posting_count, country_count FROM analytics_top_skills_global ORDER BY skill_mentions DESC LIMIT 50;" \
  "analytics_top_skills_global.csv"

export_csv \
  "SELECT job_type, posting_count, posting_pct FROM analytics_job_type_distribution ORDER BY posting_count DESC;" \
  "analytics_job_type_distribution.csv"

export_csv \
  "SELECT company, posting_count, country_count, position_count FROM analytics_top_companies ORDER BY posting_count DESC LIMIT 50;" \
  "analytics_top_companies.csv"

echo "Hive analytics stage completed."
