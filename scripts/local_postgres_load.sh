#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

DB_NAME="${DB_NAME:-linkedin_jobs}"
DB_USER="${DB_USER:-team32}"
CONTAINER_NAME="${CONTAINER_NAME:-linkedin_bigdata_postgres}"

mkdir -p "$PROJECT_ROOT/output"

echo "Creating PostgreSQL tables..."
docker exec -i "$CONTAINER_NAME" psql -U "$DB_USER" -d "$DB_NAME" \
  -v ON_ERROR_STOP=1 \
  -f /workspace/sql/postgres/01_create_tables.sql

echo "Loading CSV files into PostgreSQL..."
docker exec -i "$CONTAINER_NAME" psql -U "$DB_USER" -d "$DB_NAME" \
  -v ON_ERROR_STOP=1 \
  -f /workspace/sql/postgres/02_load_data_local.sql

echo "Running quality checks..."
docker exec -i "$CONTAINER_NAME" psql -U "$DB_USER" -d "$DB_NAME" \
  -f /workspace/sql/postgres/03_quality_checks.sql \
  | tee "$PROJECT_ROOT/output/stage1_local_quality_checks.txt"

echo "Local PostgreSQL load completed."