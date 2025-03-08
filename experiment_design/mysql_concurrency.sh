#!/bin/bash
TIMEFORMAT='%R %U %S'
MYSQL_USER="root"
MYSQL_PASSWORD="pwd"
MYSQL_DATABASE="employees_index_smaller"
MYSQL_HOST="localhost"
MYSQL_PORT=3306

# Queries
QUERY_T1="SELECT SUM(balance) FROM accounts;"
QUERY_T2=$(cat <<EOF
START TRANSACTION;
SET @valX = (SELECT balance FROM accounts WHERE number=1);
SET @valY = (SELECT balance FROM accounts WHERE number=2);
UPDATE accounts SET balance=@valX WHERE number=2;
UPDATE accounts SET balance=@valY WHERE number=1;
COMMIT;
EOF
)

echo "MYSQL QUERY STARTED"
echo "MYSQL database: $MYSQL_DATABASE" > output_mysql.txt
echo "Execution Time      Response Time" >> output_mysql.txt

# Function to run the experiment with a given isolation level
run_experiment() {
    ISOLATION_LEVEL=$1
    echo "Running with isolation level: $ISOLATION_LEVEL"
    echo "Isolation Level: $ISOLATION_LEVEL" >> output_mysql.txt

    for i in $(seq 1 11); do
        echo "Iteration ${i}..."

        # Set Isolation Level
        /data/aa10733/mysql/bin/mysql \
            -u "$MYSQL_USER" -p"$MYSQL_PASSWORD" \
            -h "$MYSQL_HOST" -P "$MYSQL_PORT" \
            "$MYSQL_DATABASE" -e "SET SESSION TRANSACTION ISOLATION LEVEL $ISOLATION_LEVEL;" > /dev/null

        # Run T1 in the background
        { time /data/aa10733/mysql/bin/mysql \
          -u "$MYSQL_USER" -p"$MYSQL_PASSWORD" \
          -h "$MYSQL_HOST" -P "$MYSQL_PORT" \
          "$MYSQL_DATABASE" -e "$QUERY_T1" > /dev/null; } 2> time_output_t1.log &

        # Run T2 concurrently in the background
        { time /data/aa10733/mysql/bin/mysql \
          -u "$MYSQL_USER" -p"$MYSQL_PASSWORD" \
          -h "$MYSQL_HOST" -P "$MYSQL_PORT" \
          "$MYSQL_DATABASE" -e "$QUERY_T2" > /dev/null; } 2> time_output_t2.log &

        # Wait for both queries to finish
        wait

        # Error handling
        if [ $? -ne 0 ]; then
            echo "Error: Query Execution Failed."
            exit 1
        fi

        # Extract execution times
        TIME_LINE_T1=$(grep -E '^[0-9.]+ +[0-9.]+ +[0-9.]+' time_output_t1.log | tail -n1)
        TIME_LINE_T2=$(grep -E '^[0-9.]+ +[0-9.]+ +[0-9.]+' time_output_t2.log | tail -n1)

        if [ -z "$TIME_LINE_T1" ] || [ -z "$TIME_LINE_T2" ]; then
            echo "Error: Execution time not found in logs"
            exit 1
        fi

        read REAL_TIME_T1 USER_TIME_T1 SYS_TIME_T1 <<< "$TIME_LINE_T1"
        read REAL_TIME_T2 USER_TIME_T2 SYS_TIME_T2 <<< "$TIME_LINE_T2"

        EXECUTION_TIME_T1=$(echo "scale=6; $USER_TIME_T1 + $SYS_TIME_T1" | bc)
        EXECUTION_TIME_T2=$(echo "scale=6; $USER_TIME_T2 + $SYS_TIME_T2" | bc)

        echo "Iteration ${i} completed for $ISOLATION_LEVEL."
        echo "$ISOLATION_LEVEL - T1 Execution Time: ${EXECUTION_TIME_T1}, Response Time: ${REAL_TIME_T1}" >> output_mysql.txt
        echo "$ISOLATION_LEVEL - T2 Execution Time: ${EXECUTION_TIME_T2}, Response Time: ${REAL_TIME_T2}" >> output_mysql.txt

        rm time_output_t1.log
        rm time_output_t2.log
    done
}

# Run the experiment for READ COMMITTED
run_experiment "READ COMMITTED"

# Run the experiment for SERIALIZABLE
run_experiment "SERIALIZABLE"

echo "MYSQL QUERY DONE"
