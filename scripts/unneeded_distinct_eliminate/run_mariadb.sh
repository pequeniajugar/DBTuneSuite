#!/bin/bash
# Benchmark: Unneeded DISTINCT on MariaDB
# Output goes into a CSV file via base_mariadb.sh with labels

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

database_name="student_techdept_small"      # change if needed
output_csv="$SCRIPT_DIR/../results/mariadb_unneeded_distinct.csv"  # change if needed

# Ensure results directory exists
mkdir -p "$(dirname "$output_csv")"

# Recreate CSV header
echo "dbms,label,iteration,execution_time,response_time" > "$output_csv"

# With DISTINCT
bash "$SCRIPT_DIR/../base_mariadb.sh" \
  "$database_name" \
  "SELECT DISTINCT ssnum FROM employee, techdept WHERE employee.dept = techdept.dept;" \
  "With Distinct" \
  "$(basename "$output_csv")"

# Without DISTINCT
bash "$SCRIPT_DIR/../base_mariadb.sh" \
  "$database_name" \
  "SELECT ssnum FROM employee, techdept WHERE employee.dept = techdept.dept;" \
  "Without Distinct" \
  "$(basename "$output_csv")"

