#!/bin/bash
# Benchmark: Clustered vs non-clustered vs no-index on PostgreSQL (point)
# Output goes into a CSV file via base_postgres.sh with labels.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

database_name="employees"  # change if needed
output_csv="$SCRIPT_DIR/../../results/postgres_clustered_nonclustered_noindex_point.csv"  # change if needed

# Ensure results directory exists
mkdir -p "$(dirname "$output_csv")"

# Recreate CSV header
echo "dbms,label,iteration,execution_time,response_time" > "$output_csv"

###############################################################################
# 1. Clustered index on ssnum
###############################################################################

bash "$SCRIPT_DIR/../../base_postgres.sh" \
  "$database_name" \
  "SET enable_seqscan = off; SELECT * FROM employee_ssnum WHERE ssnum = {v1};" \
  "clustered" \
  "$(basename "$output_csv")"

###############################################################################
# 2. Non-clustered index on ssnumpermuted1
###############################################################################

bash "$SCRIPT_DIR/../../base_postgres.sh" \
  "$database_name" \
  "SET enable_seqscan = off; SELECT * FROM employee_ssnum WHERE ssnumpermuted1 = {v1};" \
  "non-clustered" \
  "$(basename "$output_csv")"

###############################################################################
# 3. No index usage on ssnumpermuted2
###############################################################################

bash "$SCRIPT_DIR/../../base_postgres.sh" \
  "$database_name" \
  "SELECT * FROM employee_ssnum WHERE ssnumpermuted2 = {v1};" \
  "noindex" \
  "$(basename "$output_csv")"

echo "PostgreSQL clustered/non-clustered/noindex (point) benchmark finished. Results saved to: $output_csv"

