#!/bin/bash
# Benchmark: Correlated Subquery vs alternatives on DuckDB
# Output goes into a CSV file via base_duckdb.sh with labels

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

duckdb_db="/path to /student_techdept_small"  # change if needed
output_csv="$SCRIPT_DIR/../results/duckdb_correlated_subquery.csv"  # change if needed

# Ensure results directory exists
mkdir -p "$(dirname "$output_csv")"

# Recreate CSV header
echo "dbms,label,iteration,execution_time,response_time" > "$output_csv"

# Correlated Subquery
bash "$SCRIPT_DIR/../base_duckdb.sh" \
  "$duckdb_db" \
  "SELECT ssnum FROM employee e1 WHERE salary = (SELECT max(salary) FROM employee e2 WHERE e2.dept = e1.dept);" \
  "Correlated Subquery" \
  "$(basename "$output_csv")"

# Using Temporary Table (JOIN on pre-aggregated results)
bash "$SCRIPT_DIR/../base_duckdb.sh" \
  "$duckdb_db" \
  "SELECT e1.ssnum FROM employee e1 JOIN ( SELECT dept, MAX(salary) AS bigsalary FROM employee GROUP BY dept) e2 ON e1.dept = e2.dept AND e1.salary = e2.bigsalary;" \
  "Using Temporary Table" \
  "$(basename "$output_csv")"

# Using WITH Clause
bash "$SCRIPT_DIR/../base_duckdb.sh" \
  "$duckdb_db" \
  "WITH max_salary_per_dept AS ( SELECT dept, MAX(salary) AS bigsalary FROM employee GROUP BY dept ) SELECT e1.ssnum FROM employee e1 JOIN max_salary_per_dept m ON e1.dept = m.dept AND e1.salary = m.bigsalary;" \
  "Using WITH Clause" \
  "$(basename "$output_csv")"

