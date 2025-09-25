#!/bin/bash
# Benchmark: Hash vs B+Tree vs No Index on MariaDB (Range Query)
# Output goes directly into a CSV file (using base_mariadb.sh)

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

database_name="employees_index"   # Change if needed
#change file name to mariadb_small_hash_vs_btree_range.csv
output_csv="$SCRIPT_DIR/../results/mariadb_big_hash_vs_btree_range.csv"

# Ensure results directory exists
mkdir -p "$(dirname "$output_csv")"

# Recreate CSV header
echo "dbms,label,iteration,execution_time,response_time" > "$output_csv"

# --- No Index ---
echo "===== NO INDEX BENCHMARK ====="
bash "$SCRIPT_DIR/../configure_mariadb.sh" "$database_name" \
  "SET GLOBAL max_heap_table_size = 17179869184; \
   SET GLOBAL tmp_table_size = 17179869184; \
   ALTER TABLE employees ENGINE = MEMORY;"

bash "$SCRIPT_DIR/../base_mariadb.sh" \
  "$database_name" \
  "SELECT * FROM employees WHERE lat BETWEEN {v1} AND {v2};" \
  "no_index" \
  "$(basename "$output_csv")"

# --- Hash Index ---
echo "===== HASH INDEX BENCHMARK ====="
bash "$SCRIPT_DIR/../configure_mariadb.sh" "$database_name" \
  "CREATE INDEX idx_longitude_hash ON employees (longitude) USING HASH;"

bash "$SCRIPT_DIR/../base_mariadb.sh" \
  "$database_name" \
  "SELECT * FROM employees FORCE INDEX (idx_longitude_hash) WHERE longitude BETWEEN {v1} AND {v2};" \
  "hash_index" \
  "$(basename "$output_csv")"

bash "$SCRIPT_DIR/../configure_mariadb.sh" "$database_name" \
  "DROP INDEX IF EXISTS idx_longitude_hash;"

# --- B+ Tree Index ---
echo "===== BTREE INDEX BENCHMARK ====="
bash "$SCRIPT_DIR/../configure_mariadb.sh" "$database_name" \
  "CREATE INDEX idx_longitude_btree ON employees (longitude) USING BTREE;"

bash "$SCRIPT_DIR/../base_mariadb.sh" \
  "$database_name" \
  "SELECT * FROM employees FORCE INDEX (idx_longitude_btree) WHERE longitude BETWEEN {v1} AND {v2};" \
  "btree_index" \
  "$(basename "$output_csv")"

bash "$SCRIPT_DIR/../configure_mariadb.sh" "$database_name" \
  "DROP INDEX IF EXISTS idx_longitude_btree;"

# Restore InnoDB and reset memory limits
bash "$SCRIPT_DIR/../configure_mariadb.sh" "$database_name" \
  "ALTER TABLE employees ENGINE = InnoDB; \
   SET GLOBAL max_heap_table_size = 16777216; \
   SET GLOBAL tmp_table_size = 16777216;"

echo "Benchmark finished. Results saved to: $output_csv"
