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

echo "Checking Python PostgreSQL dependency..."

python3 - <<'PY'
import psycopg2
print("psycopg2 is available")
PY

echo "Running PostgreSQL ingestion via Python..."

python3 "$PROJECT_ROOT/scripts/load_postgres.py"

echo "Stage 1 PostgreSQL load completed."