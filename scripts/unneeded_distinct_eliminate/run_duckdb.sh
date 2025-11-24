#!/bin/bash
# Benchmark: Unneeded DISTINCT on DuckDB
# Output goes into a CSV file via base_duckdb.sh with labels

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

duckdb_path="/data/tw3090/duckdb/student_techdept_small.duckdb"   # change if needed
output_csv="$SCRIPT_DIR/../results/duckdb_unneeded_distinct.csv"  # change if needed

# Ensure results directory exists
mkdir -p "$(dirname "$output_csv")"

# Recreate CSV header
echo "dbms,label,iteration,execution_time,response_time" > "$output_csv"

# With DISTINCT
bash "$SCRIPT_DIR/../base_duckdb.sh" \
  "$duckdb_path" \
  "SELECT DISTINCT ssnum FROM employee, techdept WHERE employee.dept = techdept.dept;" \
  "With Distinct" \
  "$(basename "$output_csv")"

# Without DISTINCT
bash "$SCRIPT_DIR/../base_duckdb.sh" \
  "$duckdb_path" \
  "SELECT ssnum FROM employee, techdept WHERE employee.dept = techdept.dept;" \
  "Without Distinct" \
  "$(basename "$output_csv")"

