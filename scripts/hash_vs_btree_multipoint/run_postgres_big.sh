#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

database_name="employees_index" # change name
output_file="$SCRIPT_DIR/../results/postgres_big_hash_vs_btree_multipoint.txt"

echo "-----No Index-----" > "$output_file"
bash "$SCRIPT_DIR/../base_postgres.sh" "$database_name" "SELECT * FROM employees WHERE hundreds1 = 150;" >> "$output_file"

echo "-----Hash Index-----" >> "$output_file"
bash "$SCRIPT_DIR/../configure_postgres.sh" "$database_name" "CREATE INDEX idx_hundreds1_hash ON employees USING HASH (hundreds1);"
bash "$SCRIPT_DIR/../base_postgres.sh" "$database_name" "SET enable_seqscan = off; SELECT * FROM employees WHERE hundreds1 = 150;" >> "$output_file"
bash "$SCRIPT_DIR/../configure_postgres.sh" "$database_name" "DROP INDEX IF EXISTS idx_hundreds1_hash;"

echo "-----B+ Tree Index-----" >> "$output_file"
bash "$SCRIPT_DIR/../configure_postgres.sh" "$database_name" "CREATE INDEX idx_hundreds1_btree ON employees USING BTREE (hundreds1);"
bash "$SCRIPT_DIR/../base_postgres.sh" "$database_name" "SET enable_seqscan = off; SELECT * FROM employees WHERE hundreds1 = 150;" >> "$output_file"
bash "$SCRIPT_DIR/../configure_postgres.sh" "$database_name" "DROP INDEX IF EXISTS idx_hundreds1_btree;"
