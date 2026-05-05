#!/bin/bash
set -euo pipefail

echo "Stage 2: Hive raw, Parquet and enriched tables"
bash scripts/stage2_hive_tables.sh

echo "Stage 2 completed."