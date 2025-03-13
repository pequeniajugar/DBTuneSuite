#!/bin/bash

TIMEFORMAT='%R %U %S'
MYSQL_USER="root"
MYSQL_PASSWORD="pwd"
MYSQL_DATABASE="experiment_db"
MYSQL_HOST="localhost"
MYSQL_PORT=3306
POOL_SIZE=10  # Adjust pool size for pooling mode
MAX_CONNECTIONS=60
NUM_THREADS=100  # Number of simulated client threads
RETRY_WAIT=15  # Seconds to wait before retrying if connection fails

QUERY=$(cat <<EOF
insert into employees_index values (1003505,'polo94064',97.48,84.03,4700,3987);
EOF
)

# Function to perform inserts with retry logic
run_experiment() {
    local mode=$1
    local thread_id=$2
    local attempt=0

    if [ "$mode" == "pool" ]; then
        CONNECTION_OPTION="--pooling=1 --pool-size=$POOL_SIZE"
    else
        CONNECTION_OPTION=""
    fi

    while [ $attempt -lt 5 ]; do
        { time /usr/local/stow/mariadb-11.4/bin/mysql \
            -u "$MYSQL_USER" -p"$MYSQL_PASSWORD" \
            -h "$MYSQL_HOST" -P "$MYSQL_PORT" \
            "$MYSQL_DATABASE" $CONNECTION_OPTION -e "$QUERY" > /dev/null; } 2> time_output.log

        # Check if connection failed
        if grep -q "ERROR 1040" time_output.log; then
            echo "[Thread $thread_id] Connection refused, retrying in $RETRY_WAIT seconds..."
            sleep $RETRY_WAIT
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
            echo "[Thread $thread_id] ${EXECUTION_TIME} sec execution | ${REAL_TIME} sec response | Mode: ${mode}"

            ((attempt++))  # Only increment on successful execution
        fi
    done

    rm time_output.log
}

echo "-------------------------------------"
echo "Running experiment: Connection Pooling vs Simple Connections"
echo "Simulating $NUM_THREADS client threads..."
echo "Each thread performs 5 INSERTs."
echo "Max DB connections: $MAX_CONNECTIONS"
echo "-------------------------------------"

# Run experiment for both modes (Simple and Connection Pooling)
for mode in "simple" "pool"; do
    echo "Starting experiment in ${mode} mode..."
    
    active_connections=0

    thread_id=1
    while [ $thread_id -le $NUM_THREADS ]; do
        if [ "$active_connections" -ge "$MAX_CONNECTIONS" ]; then
            echo "Max connections reached, waiting for slots..."
            wait  # Wait for some threads to finish before starting new ones
            active_connections=0
        fi
        
        run_experiment "$mode" "$thread_id" &  # Run in background
        ((active_connections++))
        ((thread_id++))
    done

    wait  # Ensure all threads complete
done

echo "-------------------------------------"
echo "Experiment completed."
