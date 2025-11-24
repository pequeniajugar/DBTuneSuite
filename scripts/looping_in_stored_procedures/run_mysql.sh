#!/bin/bash
# Run retrieve-needed-columns experiments for MySQL using base_mysql.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

database_name="tpch_big"                       # change if needed
output_csv_name="mysql_big_looping.csv"  # change if needed

# Fetch all columns
bash "${SCRIPT_DIR}/../base_mysql.sh" \
  "$database_name" \
  "SELECT * FROM lineitem WHERE l_partkey < 200;" \
  "Without Loop" \
  "$output_csv_name"

# Fetch only needed columns
bash "${SCRIPT_DIR}/../base_mysql.sh" \
  "$database_name" \
  "CALL get_lineitems();" \
  "With Loop" \
  "$output_csv_name"
