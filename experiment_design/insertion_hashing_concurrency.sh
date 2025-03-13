#!/bin/bash
TIMEFORMAT='%R %U %S'

MYSQL_USER="root"
MYSQL_PASSWORD="pwd"
MYSQL_DATABASE="employees_index_smaller"
MYSQL_HOST="localhost"
MYSQL_PORT=3306
TOTAL_INSERTS=100000
MAX_THREADS= 10

OUTPUT_FILE="hashing_output.txt"
echo "HASHING INSERTION EXPERIMENT" > "$OUTPUT_FILE"

# Initialize variables to track total execution and response time
TOTAL_EXECUTION_TIME=0
TOTAL_RESPONSE_TIME=0

# Ensure index exists
setup_index() {
    /data/aa10733/mysql/bin/mysql -u "$MYSQL_USER" -p"$MYSQL_PASSWORD" -h "$MYSQL_HOST" -P "$MYSQL_PORT" "$MYSQL_DATABASE" -e "
        ALTER TABLE employees ADD COLUMN att INT;
        UPDATE employees SET att = ssnum % 1021;
        ALTER TABLE employees DROP INDEX IF EXISTS idx_ssnum_att;
        CREATE UNIQUE INDEX idx_ssnum_att ON employees(ssnum, att);
    "
}

cleanup_index() {
    /data/aa10733/mysql/bin/mysql -u "$MYSQL_USER" -p"$MYSQL_PASSWORD" -h "$MYSQL_HOST" -P "$MYSQL_PORT" "$MYSQL_DATABASE" -e "
        ALTER TABLE employees DROP COLUMN IF EXISTS att;
        ALTER TABLE employees DROP INDEX IF EXISTS idx_ssnum_att;
    "
}

# Insert function
perform_insert() {
    SSNUM=$((RANDOM % 1000000 + 1))
    HASH_KEY=$((SSNUM % 1021))  # Hashing key with 1021 possible values
    QUERY="INSERT INTO employees (ssnum, name, lat, long, hundreds1, hundreds2, att) 
           VALUES ($SSNUM, 'Employee_$SSNUM', RAND()*100, RAND()*100, RANDOM, RANDOM, $HASH_KEY);"
    
    { time /data/aa10733/mysql/bin/mysql -u "$MYSQL_USER" -p"$MYSQL_PASSWORD" -h "$MYSQL_HOST" -P "$MYSQL_PORT" "$MYSQL_DATABASE" -e "$QUERY" > /dev/null; } 2>> time_output.log

    # Check if connection failed
    if grep -q "ERROR 1040" time_output.log; then
        echo "[Thread $SSNUM] Connection refused, retrying in 5 seconds..."
        sleep 5
    else
        TIME_LINE=$(grep -E '^[0-9.]+ +[0-9.]+ +[0-9.]+' time_output.log | tail -n 1)
        if [ -z "$TIME_LINE" ]; then
            echo "Error: Could not find time in time_output.log"
            exit 1
        fi

        read REAL_TIME USER_TIME SYS_TIME <<< "$TIME_LINE"

        REAL_TIME=$(printf "%.6f" "$REAL_TIME")
        USER_TIME=$(printf "%.6f" "$USER_TIME")
        SYS_TIME=$(printf "%.6f" "$SYS_TIME")

        EXECUTION_TIME=$(echo "scale=6; $USER_TIME + $SYS_TIME" | bc)

        # Add to the total execution and response times
        TOTAL_EXECUTION_TIME=$(echo "$TOTAL_EXECUTION_TIME + $EXECUTION_TIME" | bc)
        TOTAL_RESPONSE_TIME=$(echo "$TOTAL_RESPONSE_TIME + $REAL_TIME" | bc)

        echo "[Thread $SSNUM] ${EXECUTION_TIME} sec execution | ${REAL_TIME} sec response"
    fi
}

setup_index

# Run inserts with hash-based distribution
for ((i=1; i<=TOTAL_INSERTS; i++)); do
    perform_insert &
    if ((i % MAX_THREADS == 0)); then
        wait
    fi
done

wait

# Output the total execution and response times
echo "Total Execution Time: $TOTAL_EXECUTION_TIME sec" >> "$OUTPUT_FILE"
echo "Total Response Time: $TOTAL_RESPONSE_TIME sec" >> "$OUTPUT_FILE"
echo "Hashing Insert Completed." >> "$OUTPUT_FILE"

cleanup_index
