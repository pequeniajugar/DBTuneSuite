#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
output_file="$SCRIPT_DIR/../results/mysql_big_correlated_subquery.txt"
database_name="student_techdept_big"      #change name

echo "-----Correlated Subquery-----" > "$output_file"
bash ../base_mysql.sh "$database_name" "SELECT ssnum FROM employee e1 WHERE salary = (SELECT max(salary) FROM employee e2 WHERE e2.dept = e1.dept);" >> "$output_file"

echo "-----Using Temporary Table-----" > "$output_file"
bash ../base_mysql.sh "$database_name" "SELECT e1.ssnum FROM employee e1 JOIN ( SELECT dept, MAX(salary) AS bigsalary FROM employee GROUP BY dept) e2 ON e1.dept = e2.dept AND e1.salary = e2.bigsalary;" >> "$output_file"

echo "-----Using WITH Clause-----" > "$output_file"
bash ../base_mysql.sh "$database_name" "WITH max_salary_per_dept AS ( SELECT dept, MAX(salary) AS bigsalary FROM employee GROUP BY dept ) SELECT e1.ssnum FROM employee e1 JOIN max_salary_per_dept m ON e1.dept = m.dept AND e1.salary = m.bigsalary;" >> "$output_file"
