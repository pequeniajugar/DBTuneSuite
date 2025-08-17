#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
output_file="$SCRIPT_DIR/../results/mysql_small_vp_scan.txt"
database_name="generated1"      #change name

echo "-----Without Vertical Partitioning-----" > "$output_file"

echo "---Access All Fields---" >> "$output_file"
bash ../base_mysql.sh "$database_name" "SELECT * FROM account;" >> "$output_file"

echo "---Access Partial Fields---" >> "$output_file"
bash ../base_mysql.sh "$database_name" "SELECT id, balance FROM account;" >> "$output_file"

echo "-----With Vertical Partitioning-----" > "$output_file"

echo "---Access All Fields---" >> "$output_file"
bash ../base_mysql.sh "$database_name" "Select account1.id, balance, homeaddress from account1, account2 where account1.id = account2.id;" >> "$output_file"

echo "---Access Partial Fields---" >> "$output_file"
bash ../base_mysql.sh "$database_name" "SELECT id, balance FROM account1;" >> "$output_file"
