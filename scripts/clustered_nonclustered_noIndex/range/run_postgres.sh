#!/bin/bash
# Benchmark: Clustered vs non-clustered vs no-index on PostgreSQL (range)
# Output goes into a CSV file via base_postgres.sh with labels.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

database_name="employees"  # change if needed
output_csv="$SCRIPT_DIR/../../results/postgres_clustered_nonclustered_noindex_range.csv"  # change if needed

# Ensure results directory exists
mkdir -p "$(dirname "$output_csv")"

# Recreate CSV header
echo "dbms,label,iteration,execution_time,response_time" > "$output_csv"

###############################################################################
# 1. Clustered index on ssnum (range query)
###############################################################################

bash "$SCRIPT_DIR/../../base_postgres.sh" \
  "$database_name" \
  "SET enable_seqscan = off; SELECT * FROM employee_ssnum WHERE ssnum BETWEEN {v1} AND {v2};" \
  "clustered" \
  "$(basename "$output_csv")"

###############################################################################
# 2. Non-clustered index on ssnumpermuted1 (range query)
###############################################################################

bash "$SCRIPT_DIR/../../base_postgres.sh" \
  "$database_name" \
  "SET enable_seqscan = off; SELECT * FROM employee_ssnum WHERE ssnumpermuted1 BETWEEN {v1} AND {v2};" \
  "non-clustered" \
  "$(basename "$output_csv")"

###############################################################################
# 3. No index usage on ssnumpermuted2 (range query)
###############################################################################

bash "$SCRIPT_DIR/../../base_postgres.sh" \
  "$database_name" \
  "SELECT * FROM employee_ssnum WHERE ssnumpermuted2 BETWEEN {v1} AND {v2};" \
  "noindex" \
  "$(basename "$output_csv")"

echo "PostgreSQL clustered/non-clustered/noindex (range) benchmark finished. Results saved to: $output_csv"

