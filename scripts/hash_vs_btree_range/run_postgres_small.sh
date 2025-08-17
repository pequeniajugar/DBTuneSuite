#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

database_name="employees_index_smaller" # change name
output_file="$SCRIPT_DIR/../results/postgres_small_hash_vs_btree_range.txt"

echo "-----No Index-----" >> "$output_file"
bash "$SCRIPT_DIR/../base_postgres.sh" "$database_name" "SELECT * FROM employees WHERE lat BETWEEN 150 AND 160;" >> "$output_file"

echo "-----Hash Index-----" >> "$output_file"
bash "$SCRIPT_DIR/../configure_postgres.sh" "$database_name" "CREATE INDEX idx_longitude_hash ON employees USING HASH (longitude);"
bash "$SCRIPT_DIR/../base_postgres.sh" "$database_name" "SET enable_seqscan = off; SELECT * FROM employees WHERE longitude BETWEEN 150 AND 160;" >> "$output_file"
bash "$SCRIPT_DIR/../configure_postgres.sh" "$database_name" "DROP INDEX IF EXISTS idx_longitude_hash;"

echo "-----B+ Tree Index-----" >> "$output_file"
bash "$SCRIPT_DIR/../configure_postgres.sh" "$database_name" "CREATE INDEX idx_longitude_btree ON employees USING BTREE (longitude);"
bash "$SCRIPT_DIR/../base_postgres.sh" "$database_name" "SET enable_seqscan = off; SELECT * FROM employees WHERE longitude BETWEEN 150 AND 160;" >> "$output_file"
bash "$SCRIPT_DIR/../configure_postgres.sh" "$database_name" "DROP INDEX IF EXISTS idx_longitude_btree;"
