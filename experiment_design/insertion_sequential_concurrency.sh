#!/bin/bash
TIMEFORMAT='%R %U %S'

MYSQL_USER="root"
MYSQL_PASSWORD="pwd"
MYSQL_DATABASE="employees_index_smaller"
MYSQL_HOST="localhost"
MYSQL_PORT=3306
TOTAL_INSERTS=100000
MAX_THREADS=${1:-100}  # Default to 100 threads if not provided

OUTPUT_FILE="sequential_output.txt"
echo "SEQUENTIAL INSERTION EXPERIMENT" > "$OUTPUT_FILE"

# Ensure index exists
setup_index() {
    /data/aa10733/mysql/bin/mysql -u "$MYSQL_USER" -p"$MYSQL_PASSWORD" -h "$MYSQL_HOST" -P "$MYSQL_PORT" "$MYSQL_DATABASE" -e "
        ALTER TABLE employees DROP INDEX IF EXISTS idx_ssnum;
        CREATE CLUSTERED INDEX idx_ssnum ON employees(ssnum);
    "
}

# Insert function
perform_insert() {
    SSNUM=$1
    QUERY="INSERT INTO employees (ssnum, name, lat, long, hundreds1, hundreds2) 
           VALUES ($SSNUM, 'Employee_$SSNUM', RAND()*100, RAND()*100, RANDOM, RANDOM);"
    /data/aa10733/mysql/bin/mysql -u "$MYSQL_USER" -p"$MYSQL_PASSWORD" -h "$MYSQL_HOST" -P "$MYSQL_PORT" "$MYSQL_DATABASE" -e "$QUERY"
}

setup_index

# Run inserts in sorted order
for ((i=1; i<=TOTAL_INSERTS; i++)); do
    ((thread=i%MAX_THREADS))
    perform_insert "$i" &
    if [ "$thread" -eq 0 ]; then wait; fi
done

wait
echo "Sequential Insert Completed." >> "$OUTPUT_FILE"
