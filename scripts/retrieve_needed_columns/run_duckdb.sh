#!/bin/bash
# Run retrieve-needed-columns experiments for DuckDB (small TPCH) using base_duckdb.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Path to DuckDB database file (change if needed)
duckdb_db="/data/tw3090/duckdb/tpch_small.duckdb"

# Output CSV file name (change if needed)
output_csv_name="duckdb_small_retrieve_needed_columns.csv"

# Fetch all columns
bash "${SCRIPT_DIR}/../base_duckdb.sh" \
  "$duckdb_db" \
  "SELECT * FROM lineitem;" \
  "All Columns" \
  "$output_csv_name"

# Fetch only needed columns
bash "${SCRIPT_DIR}/../base_duckdb.sh" \
  "$duckdb_db" \
  "SELECT l_orderkey, l_partkey, l_suppkey, l_shipdate, l_commitdate FROM lineitem;" \
  "Needed Columns" \
  "$output_csv_name"
