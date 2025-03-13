#!/bin/bash
TIMEFORMAT='%R %U %S'

MYSQL_USER="root"
MYSQL_PASSWORD="pwd"
MYSQL_DATABASE="employees_index_smaller"
MYSQL_HOST="localhost"
MYSQL_PORT=3306
TOTAL_INSERTS=100000
MAX_THREADS=${1:-100}  # Default to 100 threads if not provided

OUTPUT_FILE="hashing_output.txt"
echo "HASHING INSERTION EXPERIMENT" > "$OUTPUT_FILE"

# Ensure index exists
setup_index() {
    /data/aa10733/mysql/bin/mysql -u "$MYSQL_USER" -p"$MYSQL_PASSWORD" -h "$MYSQL_HOST" -P "$MYSQL_PORT" "$MYSQL_DATABASE" -e "
        ALTER TABLE employees ADD COLUMN att INT;
        UPDATE employees SET att = ssnum % 1021;
        ALTER TABLE employees DROP INDEX IF EXISTS idx_ssnum_att;
        CREATE UNIQUE INDEX idx_ssnum_att ON employees(ssnum, att);
    "
}

# Insert function
perform_insert() {
    SSNUM=$((RANDOM % 1000000 + 1))
    HASH_KEY=$((SSNUM % 1021))  # Hashing key with 1021 possible values
    QUERY="INSERT INTO employees (ssnum, name, lat, long, hundreds1, hundreds2, att) 
           VALUES ($SSNUM, 'Employee_$SSNUM', RAND()*100, RAND()*100, RANDOM, RANDOM, $HASH_KEY);"
    /data/aa10733/mysql/bin/mysql -u "$MYSQL_USER" -p"$MYSQL_PASSWORD" -h "$MYSQL_HOST" -P "$MYSQL_PORT" "$MYSQL_DATABASE" -e "$QUERY"
}

setup_index

# Run inserts with hash-based distribution
for ((i=1; i<=TOTAL_INSERTS; i++)); do
    ((thread=i%MAX_THREADS))
    perform_insert &
    if [ "$thread" -eq 0 ]; then wait; fi
done

wait
echo "Hashing Insert Completed." >> "$OUTPUT_FILE"
