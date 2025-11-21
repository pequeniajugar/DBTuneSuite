#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
output_file="$SCRIPT_DIR/../results/postgres_big_unneeded_distinct.txt"
database_name="student_techdept_big"      #change name

echo "-----With Distinct-----" > "$output_file"
bash ../base_postgres.sh "$database_name" "SELECT DISTINCT ssnum FROM employee, techdept WHERE employee.dept = techdept.dept;" >> "$output_file"

echo "-----Without Distinct-----" > "$output_file"
bash ../base_postgres.sh "$database_name" "SELECT ssnum FROM employee, techdept WHERE employee.dept = techdept.dept;" >> "$output_file"
