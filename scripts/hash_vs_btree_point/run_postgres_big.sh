#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

database_name="employees_index" # change name
output_file="$SCRIPT_DIR/../results/postgres_big_hash_vs_btree_point.txt"

echo "-----No Index-----" > "$output_file"
bash "$SCRIPT_DIR/../base_postgres.sh" "$database_name" "SELECT * FROM employees WHERE ssnum = 150;" >> "$output_file"

echo "-----Hash Index-----" >> "$output_file"
bash "$SCRIPT_DIR/../configure_postgres.sh" "$database_name" "CREATE INDEX idx_ssnum_hash ON employees USING HASH (ssnum);"
bash "$SCRIPT_DIR/../base_postgres.sh" "$database_name" "SET enable_seqscan = off; SELECT * FROM employees WHERE ssnum = 150;" >> "$output_file"
bash "$SCRIPT_DIR/../configure_postgres.sh" "$database_name" "DROP INDEX IF EXISTS idx_ssnum_hash;"

echo "-----B+ Tree Index-----" >> "$output_file"
bash "$SCRIPT_DIR/../configure_postgres.sh" "$database_name" "CREATE INDEX idx_ssnum_btree ON employees USING BTREE (ssnum);"
bash "$SCRIPT_DIR/../base_postgres.sh" "$database_name" "SET enable_seqscan = off; SELECT * FROM employees WHERE ssnum = 150;" >> "$output_file"
bash "$SCRIPT_DIR/../configure_postgres.sh" "$database_name" "DROP INDEX IF EXISTS idx_ssnum_btree;"
