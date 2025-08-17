#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

database_name="employees_index" #change name
output_file="$SCRIPT_DIR/../results/mysql_big_hash_vs_btree_point.txt"

bash "$SCRIPT_DIR/../configure_mysql.sh" "$database_name" "ALTER TABLE employees ENGINE = MEMORY;"

echo "-----No Index-----" > "$output_file"
bash "$SCRIPT_DIR/../base_mysql.sh" "$database_name" "SELECT * FROM employees WHERE ssnum = 150;" >> "$output_file"

echo "-----Hash Index-----" >> "$output_file"
bash "$SCRIPT_DIR/../configure_mysql.sh" "$database_name" "CREATE INDEX idx_ssnum ON employees (ssnum) USING HASH;"
bash "$SCRIPT_DIR/../base_mysql.sh" "$database_name" "SELECT * FROM employees FORCE INDEX (idx_ssnum) WHERE ssnum = 150;" >> "$output_file"
bash "$SCRIPT_DIR/../configure_mysql.sh" "$database_name" "DROP INDEX IF EXISTS idx_ssnum;"

echo "-----B+ Tree Index-----" >> "$output_file"
bash "$SCRIPT_DIR/../configure_mysql.sh" "$database_name" "CREATE INDEX idx_ssnum ON employees (ssnum) USING BTREE;"
bash "$SCRIPT_DIR/../base_mysql.sh" "$database_name" "SELECT * FROM employees FORCE INDEX (idx_ssnum) WHERE ssnum = 150;" >> "$output_file"
bash "$SCRIPT_DIR/../configure_mysql.sh" "$database_name" "DROP INDEX IF EXISTS idx_ssnum;"

bash "$SCRIPT_DIR/../configure_mysql.sh" "$database_name" "ALTER TABLE employees ENGINE = InnoDB;"
