#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [ -f "$PROJECT_ROOT/.env" ]; then
  source "$PROJECT_ROOT/.env"
else
  echo ".env file not found. Copy .env.example to .env and configure it."
  exit 1
fi

JDBC_URL="jdbc:postgresql://${DB_HOST}:${DB_PORT}/${DB_NAME}"

echo "Creating HDFS base directory..."
hdfs dfs -mkdir -p "$HDFS_BASE/raw"

echo "Cleaning old HDFS raw folders..."
hdfs dfs -rm -r -f "$HDFS_BASE/raw/linkedin_job_postings"
hdfs dfs -rm -r -f "$HDFS_BASE/raw/job_skills"
hdfs dfs -rm -r -f "$HDFS_BASE/raw/job_summary"

echo "Importing linkedin_job_postings to HDFS..."
sqoop import \
  --connect "$JDBC_URL" \
  --username "$DB_USER" \
  --password "$DB_PASSWORD" \
  --table linkedin_job_postings \
  --target-dir "$HDFS_BASE/raw/linkedin_job_postings" \
  --as-textfile \
  --fields-terminated-by '\001' \
  --lines-terminated-by '\n' \
  --null-string '\\N' \
  --null-non-string '\\N' \
  --hive-drop-import-delims \
  --num-mappers 1

echo "Importing job_skills to HDFS..."
sqoop import \
  --connect "$JDBC_URL" \
  --username "$DB_USER" \
  --password "$DB_PASSWORD" \
  --table job_skills \
  --target-dir "$HDFS_BASE/raw/job_skills" \
  --as-textfile \
  --fields-terminated-by '\001' \
  --lines-terminated-by '\n' \
  --null-string '\\N' \
  --null-non-string '\\N' \
  --hive-drop-import-delims \
  --num-mappers 1

echo "Importing job_summary to HDFS..."
sqoop import \
  --connect "$JDBC_URL" \
  --username "$DB_USER" \
  --password "$DB_PASSWORD" \
  --table job_summary \
  --target-dir "$HDFS_BASE/raw/job_summary" \
  --as-textfile \
  --fields-terminated-by '\001' \
  --lines-terminated-by '\n' \
  --null-string '\\N' \
  --null-non-string '\\N' \
  --hive-drop-import-delims \
  --num-mappers 1

echo "Checking imported HDFS folders..."
hdfs dfs -ls "$HDFS_BASE/raw"

echo "Counting imported HDFS lines..."
hdfs dfs -cat "$HDFS_BASE/raw/linkedin_job_postings/part-*" | wc -l
hdfs dfs -cat "$HDFS_BASE/raw/job_skills/part-*" | wc -l
hdfs dfs -cat "$HDFS_BASE/raw/job_summary/part-*" | wc -l

echo "Sqoop import completed."