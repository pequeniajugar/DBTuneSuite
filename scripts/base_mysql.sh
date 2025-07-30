#!/bin/bash
TIMEFORMAT='%R %U %S'
MYSQL_USER="root" # Placeholder
MYSQL_PASSWORD="pwd" # Placeholder
MYSQL_HOST="localhost" # Placeholder
MYSQL_PORT=3306 # Placeholder
MYSQL_DATABASE="$1"
QUERY="$2"
echo "MYSQL QUERY STARTED"
echo "MYSQL database: $MYSQL_DATABASE" > mysql_output.txt
echo "MYSQL Query: $QUERY" >> mysql_output.txt
echo "Execution Time      Response Time" >> mysql_output.txt
for i in $(seq 1 11); do
    { time /data/aa10733/mysql/bin/mysql \
      -u "$MYSQL_USER" -p"$MYSQL_PASSWORD" \
      -h "$MYSQL_HOST" -P "$MYSQL_PORT" \
      "$MYSQL_DATABASE" -e "$QUERY" > /dev/null; } 2> mysql_time_output.log

      if [ $? -ne 0 ]; then
        echo "Error: Connection to MySQL server failed."
        echo "MySQL Error Details:"
        cat mysql_time_output.log
        rm mysql_time_output.log
        exit 1
    fi
    TIME_LINE=$(grep -E '^[0-9.]+ +[0-9.]+ +[0-9.]+' mysql_time_output.log | tail -n 1)
    if [ -z "$TIME_LINE" ]; then
        echo "Error: not find time in time_output.log"
        exit 1
    fi

    read REAL_TIME USER_TIME SYS_TIME <<< "$TIME_LINE"

    REAL_TIME=$(printf "%.6f" "$REAL_TIME")
    USER_TIME=$(printf "%.6f" "$USER_TIME")
    SYS_TIME=$(printf "%.6f" "$SYS_TIME")

    EXECUTION_TIME=$(echo "scale=6; $USER_TIME + $SYS_TIME" | bc)
    echo "ran mysql ${i}"
    echo "${EXECUTION_TIME}            ${REAL_TIME}" >> mysql_output.txt
    rm mysql_time_output.log
done
echo "MYSQL QUERY DONE"
