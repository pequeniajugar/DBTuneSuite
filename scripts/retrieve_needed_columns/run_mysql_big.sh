#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
output_file="$SCRIPT_DIR/../results/mysql_big_retrieve_needed_columns.txt"
database_name="tpch_big"      #change name

echo "-----Fetch All Columns-----" > "$output_file"
bash ../base_mysql.sh "$database_name" "SELECT * FROM lineitem;" >> "$output_file"

echo "-----Fetch Needed Columns-----" > "$output_file"
bash ../base_mysql.sh "$database_name" "SELECT l_orderkey, l_partkey, l_suppkey, l_shipdate, l_commitdate FROM lineitem;" >> "$output_file"
