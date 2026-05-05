#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [ -f "$PROJECT_ROOT/.env" ]; then
  source "$PROJECT_ROOT/.env"
else
  echo ".env file not found. Copy .env.example to .env and configure it."
  exit 1
fi

mkdir -p "$PROJECT_ROOT/output"

echo "Creating Hive raw external tables..."

hive \
  --hiveconf HIVE_DB="$HIVE_DB" \
  --hiveconf HDFS_BASE="$HDFS_BASE" \
  -f "$PROJECT_ROOT/sql/hive/01_create_raw_tables.hql"

echo "Creating Hive Parquet tables..."

hive \
  --hiveconf HIVE_DB="$HIVE_DB" \
  --hiveconf HDFS_BASE="$HDFS_BASE" \
  -f "$PROJECT_ROOT/sql/hive/02_create_parquet_tables.hql"

echo "Creating Hive enriched table..."

hive \
  --hiveconf HIVE_DB="$HIVE_DB" \
  --hiveconf HDFS_BASE="$HDFS_BASE" \
  -f "$PROJECT_ROOT/sql/hive/03_create_enriched_table.hql"

echo "Checking Hive table counts..."

hive \
  --hiveconf HIVE_DB="$HIVE_DB" \
  -e "
USE $HIVE_DB;

SELECT 'linkedin_job_postings_raw' AS table_name, COUNT(*) AS row_count FROM linkedin_job_postings_raw
UNION ALL
SELECT 'job_skills_raw', COUNT(*) FROM job_skills_raw
UNION ALL
SELECT 'job_summary_raw', COUNT(*) FROM job_summary_raw
UNION ALL
SELECT 'linkedin_job_postings_parquet', COUNT(*) FROM linkedin_job_postings_parquet
UNION ALL
SELECT 'job_skills_parquet', COUNT(*) FROM job_skills_parquet
UNION ALL
SELECT 'job_summary_parquet', COUNT(*) FROM job_summary_parquet
UNION ALL
SELECT 'linkedin_jobs_enriched', COUNT(*) FROM linkedin_jobs_enriched;
" | tee "$PROJECT_ROOT/output/stage2_hive_counts.tsv"

echo "Hive stage completed."