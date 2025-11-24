#!/bin/bash
# Benchmark: Covering vs non-covering indexes on MySQL
# Output goes into a CSV file via base_mysql.sh with labels.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

database_name="employees_index"  # change if needed
output_csv="$SCRIPT_DIR/../results/mysql_covering_index.csv"  # change if needed

# Ensure results directory exists
mkdir -p "$(dirname "$output_csv")"

# Recreate CSV header
echo "dbms,label,iteration,execution_time,response_time" > "$output_csv"

###############################################################################
# 0. Ensure a clean starting index state (ordered covering nc4)
###############################################################################

bash "$SCRIPT_DIR/../configure_mysql.sh" \
  "$database_name" \
  "DROP INDEX IF EXISTS nc5 ON employees;
   CREATE INDEX IF NOT EXISTS nc4 ON employees(lat, ssnum, name);"

###############################################################################
# 1. Ordered covering index nc4 on (lat, ssnum, name)
###############################################################################

bash "$SCRIPT_DIR/../base_mysql.sh" \
  "$database_name" \
  "SELECT ssnum, name FROM employees WHERE lat = {v1};" \
  "ordered covering" \
  "$(basename "$output_csv")"

###############################################################################
# 2. Unordered covering index nc5 on (ssnum, name, lat)
###############################################################################

bash "$SCRIPT_DIR/../configure_mysql.sh" \
  "$database_name" \
  "DROP INDEX IF EXISTS nc4 ON employees;
   CREATE INDEX IF NOT EXISTS nc5 ON employees(ssnum, name, lat);"

bash "$SCRIPT_DIR/../base_mysql.sh" \
  "$database_name" \
  "SELECT ssnum, name FROM employees WHERE lat = {v1};" \
  "unordered covering" \
  "$(basename "$output_csv")"

###############################################################################
# 3. Non-clustered non-covering index c on (hundreds1)
###############################################################################

bash "$SCRIPT_DIR/../configure_mysql.sh" \
  "$database_name" \
  "DROP INDEX IF EXISTS nc4 ON employees;
   DROP INDEX IF EXISTS nc5 ON employees;
   CREATE INDEX IF NOT EXISTS c ON employees(hundreds1);"

bash "$SCRIPT_DIR/../base_mysql.sh" \
  "$database_name" \
  "SELECT * FROM employees WHERE hundreds1 = {v1};" \
  "non-clustered non-covering" \
  "$(basename "$output_csv")"

###############################################################################
# 4. Cleanup: drop nc5/c and restore nc4 (lat, ssnum, name)
###############################################################################

bash "$SCRIPT_DIR/../configure_mysql.sh" \
  "$database_name" \
  "DROP INDEX IF EXISTS nc5 ON employees;
   CREATE INDEX IF NOT EXISTS nc4 ON employees(lat, ssnum, name);"

echo "MySQL covering index benchmark finished. Results saved to: $output_csv"
