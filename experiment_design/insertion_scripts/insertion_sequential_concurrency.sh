#!/bin/bas
TIMEFORMAT='%R %U %S'

MYSQL_USER="root"
MYSQL_PASSWORD="pwd"
MYSQL_DATABASE="employees_index_smaller"
MYSQL_HOST="localhost"
MYSQL_PORT=3306
TOTAL_INSERTS=100000
MAX_THREADS=50

echo "SEQUENTIAL INSERTION EXPERIMENT" >> "sequential_output.txt"

TOTAL_EXECUTION_TIME=0.0
TOTAL_RESPONSE_TIME=0.0

setup_index() {
    /data/aa10733/mysql/bin/mysql -u "$MYSQL_USER" -p"$MYSQL_PASSWORD" -h "$MYSQL_HOST" -P "$MYSQL_PORT" "$MYSQL_DATABASE" -e "
        CREATE INDEX idx_ssnum ON employees(ssnum);
    "
}

# Insert function
perform_insert() {
    SSNUM=$1
    QUERY="INSERT INTO employees (ssnum, name, lat, long, hundreds1, hundreds2) 
           VALUES ($SSNUM, 'Employee_$SSNUM', RAND()*100, RAND()*100, RANDOM, RANDOM);"
    { time /data/aa10733/mysql/bin/mysql -u "$MYSQL_USER" -p"$MYSQL_PASSWORD" -h "$MYSQL_HOST" -P "$MYSQL_PORT" "$MYSQL_DATABASE" -e "$QUERY" > /dev/null; } 2>> time_output.log


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

        if [ $(("$SSNUM" % 1000)) -eq 0 ]; then echo "$SSNUM inserts done"; fi

    fi
}

setup_index

# Run inserts in sorted order
echo "Running inserts"
for ((i=1; i<=TOTAL_INSERTS; i++)); do
    ((thread=i%MAX_THREADS))
    perform_insert "$i" &
    if [ "$thread" -eq 0 ]; then wait; fi
done

wait
echo "Total Execution Time: $TOTAL_EXECUTION_TIME sec" >> "sequential_output.txt"
echo "Total Response Time: $TOTAL_RESPONSE_TIME sec" >> "sequential_output.txt"
echo "Sequential Insert Completed." >> "sequential_output.txt"
