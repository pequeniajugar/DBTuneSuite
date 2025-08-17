#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

database_name="employees_index" #change name
output_file="$SCRIPT_DIR/../results/mysql_big_hash_vs_btree_multipoint.txt"

bash "$SCRIPT_DIR/../configure_mysql.sh" "$database_name" "ALTER TABLE employees ENGINE = MEMORY;"
echo "-----No Index-----" > "$output_file"
bash "$SCRIPT_DIR/../base_mysql.sh" "$database_name" "SELECT * FROM employees WHERE hundreds1 = 150;" >> "$output_file"

echo "-----Hash Index-----" >> "$output_file"
bash "$SCRIPT_DIR/../configure_mysql.sh" "$database_name" "CREATE INDEX idx_hundreds1 ON employees (hundreds1) USING HASH;"
bash "$SCRIPT_DIR/../base_mysql.sh" "$database_name" "SELECT * FROM employees FORCE INDEX (idx_hundreds1) WHERE hundreds1 = 150;" >> "$output_file"
bash "$SCRIPT_DIR/../configure_mysql.sh" "$database_name" "DROP INDEX IF EXISTS idx_hundreds1;"

echo "-----B+ Tree Index-----" >> "$output_file"
bash "$SCRIPT_DIR/../configure_mysql.sh" "$database_name" "CREATE INDEX idx_hundreds1 ON employees (hundreds1) USING BTREE;"
bash "$SCRIPT_DIR/../base_mysql.sh" "$database_name" "SELECT * FROM employees FORCE INDEX (idx_hundreds1) WHERE hundreds1 = 150;" >> "$output_file"
bash "$SCRIPT_DIR/../configure_mysql.sh" "$database_name" "DROP INDEX IF EXISTS idx_hundreds1;"

bash "$SCRIPT_DIR/../configure_mysql.sh" "$database_name" "ALTER TABLE employees ENGINE = InnoDB;"
