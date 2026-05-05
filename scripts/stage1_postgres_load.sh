#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [ -f "$PROJECT_ROOT/.env" ]; then
  source "$PROJECT_ROOT/.env"
else
  echo ".env file not found. Copy .env.example to .env and configure it."
  exit 1
fi

DATA_DIR="${DATA_DIR:-$PROJECT_ROOT/data/raw}"

mkdir -p "$PROJECT_ROOT/output"

export PGPASSWORD="$DB_PASSWORD"

echo "Checking required dataset files..."

for file in linkedin_job_postings.csv job_skills.csv job_summary.csv; do
  if [ ! -f "$DATA_DIR/$file" ]; then
    echo "Missing dataset file: $DATA_DIR/$file"
    exit 1
  fi
done

echo "Creating PostgreSQL tables..."

psql \
  -h "$DB_HOST" \
  -p "$DB_PORT" \
  -U "$DB_USER" \
  -d "$DB_NAME" \
  -v ON_ERROR_STOP=1 \
  -f "$PROJECT_ROOT/sql/postgres/01_create_tables.sql"

echo "Loading linkedin_job_postings.csv..."

psql \
  -h "$DB_HOST" \
  -p "$DB_PORT" \
  -U "$DB_USER" \
  -d "$DB_NAME" \
  -v ON_ERROR_STOP=1 \
  -c "\copy linkedin_job_postings (
    job_link,
    last_processed_time,
    got_summary,
    got_ner,
    is_being_worked,
    job_title,
    company,
    job_location,
    first_seen,
    search_city,
    search_country,
    search_position,
    job_level,
    job_type
  ) FROM '$DATA_DIR/linkedin_job_postings.csv' WITH (FORMAT csv, HEADER true, QUOTE '\"', ESCAPE '\"');"

echo "Loading job_skills.csv..."

psql \
  -h "$DB_HOST" \
  -p "$DB_PORT" \
  -U "$DB_USER" \
  -d "$DB_NAME" \
  -v ON_ERROR_STOP=1 \
  -c "\copy job_skills (
    job_link,
    job_skills
  ) FROM '$DATA_DIR/job_skills.csv' WITH (FORMAT csv, HEADER true, QUOTE '\"', ESCAPE '\"');"

echo "Loading job_summary.csv..."

psql \
  -h "$DB_HOST" \
  -p "$DB_PORT" \
  -U "$DB_USER" \
  -d "$DB_NAME" \
  -v ON_ERROR_STOP=1 \
  -c "\copy job_summary (
    job_link,
    job_summary
  ) FROM '$DATA_DIR/job_summary.csv' WITH (FORMAT csv, HEADER true, QUOTE '\"', ESCAPE '\"');"

echo "Running quality checks..."

psql \
  -h "$DB_HOST" \
  -p "$DB_PORT" \
  -U "$DB_USER" \
  -d "$DB_NAME" \
  -f "$PROJECT_ROOT/sql/postgres/03_quality_checks.sql" \
  | tee "$PROJECT_ROOT/output/stage1_postgres_quality_checks.txt"

echo "Stage 1 PostgreSQL load completed."