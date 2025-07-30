#!/bin/bash
TIMEFORMAT='%R %U %S'
PGUSER="user" # Placeholder
PGHOST="localhost" # Placeholder
PGPORT=10000 # Placeholder
PGDATABASE="$1"
QUERY="$2"
echo "PG QUERY STARTED"
echo "Postgres database: $PGDATABASE" > postgres_output.txt
echo "Postgres Query: $QUERY" >> postgres_output.txt
echo "Execution Time      Response Time" >> postgres_output.txt
for i in $(seq 1 11); do
   { time /usr/bin/psql \
      -U "$PGUSER" \
      -h "$PGHOST" \
      -p "$PGPORT" \
      -d "$PGDATABASE" \
      -c "$QUERY" > /dev/null; } 2> postgres_time_output.log

    if [ $? -ne 0 ]; then
        echo "Error: Connection to PostgreSQL server failed."
        echo "psql Error Details:"
        cat postgres_time_output.log
        rm postgres_time_output.log
        exit 1
    fi

    TIME_LINE=$(grep -E '^[0-9.]+ +[0-9.]+ +[0-9.]+' postgres_time_output.log | tail -n 1)
    if [ -z "$TIME_LINE" ]; then
        echo "Error: not find time in time_output.log"
        exit 1
    fi

    read REAL_TIME USER_TIME SYS_TIME <<< "$TIME_LINE"

    REAL_TIME=$(printf "%.6f" "$REAL_TIME")
    USER_TIME=$(printf "%.6f" "$USER_TIME")
    SYS_TIME=$(printf "%.6f" "$SYS_TIME")

    EXECUTION_TIME=$(echo "scale=6; $USER_TIME + $SYS_TIME" | bc)
    echo "${EXECUTION_TIME}            ${REAL_TIME}" >> postgres_output.txt
    rm postgres_time_output.log

done
echo "PG QUERY DONE"
