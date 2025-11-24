#!/bin/bash
# Run retrieve-needed-columns experiments for MySQL using base_mysql.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

database_name="tpch_big"                       # change if needed
output_csv_name="mysql_big_retrieve_needed_columns.csv"  # change if needed

# Fetch all columns
bash "${SCRIPT_DIR}/../base_mysql.sh" \
  "$database_name" \
  "SELECT * FROM lineitem;" \
  "All Columns" \
  "$output_csv_name"

# Fetch only needed columns
bash "${SCRIPT_DIR}/../base_mysql.sh" \
  "$database_name" \
  "SELECT l_orderkey, l_partkey, l_suppkey, l_shipdate, l_commitdate FROM lineitem;" \
  "Needed Columns" \
  "$output_csv_name"
