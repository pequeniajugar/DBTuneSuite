#!/bin/bash
# Benchmark: Hash vs B+Tree vs No Index on MySQL
# Output goes directly into a CSV file (using base_mysql.sh)

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

database_name="employees_index"   # Change if needed
#change file name to mysql_small_hash_vs_btree_multipoint.csv
output_csv="$SCRIPT_DIR/../results/mysql_big_hash_vs_btree_multipoint.csv" 

# Ensure results directory exists
mkdir -p "$(dirname "$output_csv")"

# Recreate CSV header
echo "dbms,label,iteration,execution_time,response_time" > "$output_csv"

# --- No Index ---
echo "===== NO INDEX BENCHMARK ====="
bash "$SCRIPT_DIR/../configure_mysql.sh" "$database_name" \
  "SET GLOBAL max_heap_table_size = 17179869184; 
  SET GLOBAL tmp_table_size = 117179869184; 
  ALTER TABLE employees ENGINE = MEMORY;"

bash "$SCRIPT_DIR/../base_mysql.sh" \
  "$database_name" \
  "SELECT * FROM employees WHERE hundreds1 = {v1};" \
  "no_index" \
  "$(basename "$output_csv")"

# --- Hash Index ---
echo "===== HASH INDEX BENCHMARK ====="
bash "$SCRIPT_DIR/../configure_mysql.sh" "$database_name" \
  "CREATE INDEX idx_hundreds1 ON employees (hundreds1) USING HASH;"

bash "$SCRIPT_DIR/../base_mysql.sh" \
  "$database_name" \
  "SELECT * FROM employees FORCE INDEX (idx_hundreds1) WHERE hundreds1 = {v1};" \
  "hash_index" \
  "$(basename "$output_csv")"

bash "$SCRIPT_DIR/../configure_mysql.sh" "$database_name" \
  "DROP INDEX IF EXISTS idx_hundreds1;"

# --- B+ Tree Index ---
echo "===== BTREE INDEX BENCHMARK ====="
bash "$SCRIPT_DIR/../configure_mysql.sh" "$database_name" \
  "CREATE INDEX idx_hundreds1 ON employees (hundreds1) USING BTREE;"

bash "$SCRIPT_DIR/../base_mysql.sh" \
  "$database_name" \
  "SELECT * FROM employees FORCE INDEX (idx_hundreds1) WHERE hundreds1 = {v1};" \
  "btree_index" \
  "$(basename "$output_csv")"

bash "$SCRIPT_DIR/../configure_mysql.sh" "$database_name" \
  "DROP INDEX IF EXISTS idx_hundreds1;"

# Restore InnoDB
bash "$SCRIPT_DIR/../configure_mysql.sh" "$database_name" \
  "ALTER TABLE employees ENGINE = InnoDB;SET GLOBAL max_heap_table_size = 16777216; SET GLOBAL tmp_table_size = 16777216;"

echo "Benchmark finished. Results saved to: $output_csv"
