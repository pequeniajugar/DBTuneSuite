#!/bin/bash
# Run retrieve-needed-columns experiments for MySQL using base_mysql.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

database_name="employee_big"                       # change if needed
output_csv_name="mysql_big_cursor.csv"  # change if needed

# Fetch all columns
bash "${SCRIPT_DIR}/../base_mysql.sh" \
  "$database_name" \
  "SELECT * FROM employee;" \
  "Without cursor" \
  "$output_csv_name"

# Fetch only needed columns
bash "${SCRIPT_DIR}/../base_mysql.sh" \
  "$database_name" \
  "CALL fetch_employee();" \
  "With cursor" \
  "$output_csv_name"
