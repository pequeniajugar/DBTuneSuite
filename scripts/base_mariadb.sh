#!/bin/bash
# Run a single SQL query against MariaDB, 11 repetitions with timing (built-in `time`)
# usage:  bash base_mariadb.sh database_name "query(ies)" [optional_label]
# example: bash base_mariadb.sh account_new_7 "SELECT id, balance FROM account1 LIMIT 10;" queries/with_vp/n0.txt

set -euo pipefail
TIMEFORMAT='%R %U %S'

# --- Connection settings (edit as needed) ---
MYSQL_USER="tw3090"
MYSQL_PASSWORD="64113491Ka"
MYSQL_HOST="localhost"
MYSQL_PORT=15559
# -------------------------------------------

MYSQL_DB="$1"         # MariaDB database name
QUERY="$2"            # SQL query or queries
LABEL="${3-}"         # Optional label for output (e.g., relative path of query file)

# Function: prepend 0 if the number starts with a dot (e.g., .123456 -> 0.123456)
pad0() {
  local val="$1"
  if [[ "$val" =~ ^\.[0-9] ]]; then
    echo "0$val"
  else
    echo "$val"
  fi
}

# Prepare output directory and CSV file
RESULTS_DIR="./results"
mkdir -p "$RESULTS_DIR"
RESULTS_FILE="$RESULTS_DIR/mariadb_results.csv"

# Write CSV header if the file does not exist yet
if [[ ! -f "$RESULTS_FILE" ]]; then
  echo "dbms,label,iteration,execution_time,response_time" > "$RESULTS_FILE"
fi

echo "MARIADB QUERY STARTED"
echo "MariaDB database: $MYSQL_DB"
if [[ -n "$LABEL" ]]; then
  echo "Query File: $LABEL"
else
  echo "Query: $QUERY"
fi
echo "Execution Time      Response Time"

for i in $(seq 1 11); do
  # Use built-in `time` to capture real/user/sys time
  { time mysql \
      -u "$MYSQL_USER" -p"$MYSQL_PASSWORD" \
      -h "$MYSQL_HOST" -P "$MYSQL_PORT" \
      "$MYSQL_DB" -e "$QUERY" > /dev/null; } 2> mariadb_time_output.log

  # If mysql failed, print error and exit
  if [ $? -ne 0 ]; then
    echo "Error: mysql command failed."
    echo "Details:"
    cat mariadb_time_output.log || true
    rm -f mariadb_time_output.log
    exit 1
  fi

  # Extract timing information from built-in `time` output
  TIME_LINE=$(grep -E '^[0-9.]+ +[0-9.]+ +[0-9.]+' mariadb_time_output.log | tail -n 1 || true)
  if [ -z "$TIME_LINE" ]; then
    echo "Error: no timing information found in mariadb_time_output.log"
    rm -f mariadb_time_output.log
    exit 1
  fi

  read REAL_TIME USER_TIME SYS_TIME <<< "$TIME_LINE"

  # Format numbers to 6 decimal places
  REAL_TIME=$(printf "%.6f" "$REAL_TIME")
  USER_TIME=$(printf "%.6f" "$USER_TIME")
  SYS_TIME=$(printf "%.6f" "$SYS_TIME")

  # Calculate execution time as user+sys
  EXECUTION_TIME=$(echo "scale=6; $USER_TIME + $SYS_TIME" | bc)

  # Add leading zero if missing
  REAL_TIME=$(pad0 "$REAL_TIME")
  EXECUTION_TIME=$(pad0 "$EXECUTION_TIME")

  echo "ran mariadb ${i}"
  echo "${EXECUTION_TIME}            ${REAL_TIME}"

  # Append result to CSV, dbms field fixed to "mariadb"
  if [[ -n "$LABEL" ]]; then
    echo "mariadb,${LABEL},${i},${EXECUTION_TIME},${REAL_TIME}" >> "$RESULTS_FILE"
  else
    # Be careful: embedding raw SQL in CSV is fine but contains commas; Excel will still parse columns correctly
    echo "mariadb,${QUERY},${i},${EXECUTION_TIME},${REAL_TIME}" >> "$RESULTS_FILE"
  fi

  rm -f mariadb_time_output.log
done

echo "MARIADB QUERY DONE"
