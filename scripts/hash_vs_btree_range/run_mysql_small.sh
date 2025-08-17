#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

database_name="employees_index_smaller" # change name
output_file="$SCRIPT_DIR/../results/mysql_small_hash_vs_btree_range.txt"

bash "$SCRIPT_DIR/../configure_mysql.sh" "$database_name" "ALTER TABLE employees ENGINE = MEMORY;"

echo "-----No Index-----" > "$output_file"
bash "$SCRIPT_DIR/../base_mysql.sh" "$database_name" "SELECT * FROM employees WHERE lat BETWEEN 150 AND 160;" >> "$output_file"

echo "-----Hash Index-----" >> "$output_file"
bash "$SCRIPT_DIR/../configure_mysql.sh" "$database_name" "CREATE INDEX idx_longitude_hash ON employees (longitude) USING HASH;"
bash "$SCRIPT_DIR/../base_mysql.sh" "$database_name" "SELECT * FROM employees FORCE INDEX (idx_longitude_hash) WHERE longitude BETWEEN 150 AND 160;" >> "$output_file"
bash "$SCRIPT_DIR/../configure_mysql.sh" "$database_name" "DROP INDEX IF EXISTS idx_longitude_hash;"

echo "-----B+ Tree Index-----" >> "$output_file"
bash "$SCRIPT_DIR/../configure_mysql.sh" "$database_name" "CREATE INDEX idx_longitude_btree ON employees (longitude) USING BTREE;"
bash "$SCRIPT_DIR/../base_mysql.sh" "$database_name" "SELECT * FROM employees FORCE INDEX (idx_longitude_btree) WHERE longitude BETWEEN 150 AND 160;" >> "$output_file"
bash "$SCRIPT_DIR/../configure_mysql.sh" "$database_name" "DROP INDEX IF EXISTS idx_longitude_btree;"

bash "$SCRIPT_DIR/../configure_mysql.sh" "$database_name" "ALTER TABLE employees ENGINE = InnoDB;"
