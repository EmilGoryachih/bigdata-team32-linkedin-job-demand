#!/bin/bash
set -euo pipefail

echo "Stage 1: PostgreSQL/Citus load"
bash scripts/stage1_postgres_load.sh

echo "Stage 1: Sqoop import PostgreSQL tables to HDFS"
bash scripts/stage1_sqoop_import.sh

echo "Stage 1 completed."