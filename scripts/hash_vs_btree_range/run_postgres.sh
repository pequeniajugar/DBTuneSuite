#!/bin/bash
# Benchmark: Hash vs B+Tree vs No Index on PostgreSQL (Range Query)
# Output goes directly into a CSV file (using base_postgres.sh)

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

database_name="employees_index"   # Change if needed
#change file name to postgres_small_hash_vs_btree_range.csv
output_csv="$SCRIPT_DIR/../results/postgres_big_hash_vs_btree_range.csv"

# Ensure results directory exists
mkdir -p "$(dirname "$output_csv")"

# Recreate CSV header
echo "dbms,label,iteration,execution_time,response_time" > "$output_csv"

# --- No Index ---
echo "===== NO INDEX BENCHMARK ====="
bash "$SCRIPT_DIR/../base_postgres.sh" \
  "$database_name" \
  "SELECT * FROM employees WHERE lat BETWEEN {v1} AND {v2};" \
  "no_index" \
  "$(basename "$output_csv")"

# --- Hash Index ---
echo "===== HASH INDEX BENCHMARK ====="
bash "$SCRIPT_DIR/../configure_postgres.sh" "$database_name" \
  "CREATE INDEX idx_longitude_hash ON employees USING HASH (longitude);"

bash "$SCRIPT_DIR/../base_postgres.sh" \
  "$database_name" \
  "SET enable_seqscan = off; SELECT * FROM employees WHERE longitude BETWEEN {v1} AND {v2};" \
  "hash_index" \
  "$(basename "$output_csv")"

bash "$SCRIPT_DIR/../configure_postgres.sh" "$database_name" \
  "DROP INDEX IF EXISTS idx_longitude_hash;"

# --- B+ Tree Index ---
echo "===== BTREE INDEX BENCHMARK ====="
bash "$SCRIPT_DIR/../configure_postgres.sh" "$database_name" \
  "CREATE INDEX idx_longitude_btree ON employees USING BTREE (longitude);"

bash "$SCRIPT_DIR/../base_postgres.sh" \
  "$database_name" \
  "SET enable_seqscan = off; SELECT * FROM employees WHERE longitude BETWEEN {v1} AND {v2};" \
  "btree_index" \
  "$(basename "$output_csv")"

bash "$SCRIPT_DIR/../configure_postgres.sh" "$database_name" \
  "DROP INDEX IF EXISTS idx_longitude_btree;"

echo "Benchmark finished. Results saved to: $output_csv"
