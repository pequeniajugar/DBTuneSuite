#!/bin/bash
# Run retrieve-needed-columns experiments for MariaDB (small TPCH) using base_mariadb.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# MariaDB database name (change if needed)
database_name="tpch_small"

# Output CSV file name (change if needed)
output_csv_name="mariadb_small_retrieve_needed_columns.csv"

# Fetch all columns
bash "${SCRIPT_DIR}/../base_mariadb.sh" \
  "$database_name" \
  "SELECT * FROM lineitem;" \
  "All Columns" \
  "$output_csv_name"

# Fetch only needed columns
bash "${SCRIPT_DIR}/../base_mariadb.sh" \
  "$database_name" \
  "SELECT l_orderkey, l_partkey, l_suppkey, l_shipdate, l_commitdate FROM lineitem;" \
  "Needed Columns" \
  "$output_csv_name"
