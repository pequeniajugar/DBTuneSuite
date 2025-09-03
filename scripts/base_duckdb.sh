#!/bin/bash
# Run a single SQL query against DuckDB, 11 repetitions with timing
# usage:  bash base_duckdb.sh database_path "query(ies)"
# example: bash base_duckdb.sh /data/tw3090/duckdb/account_new_7 "select id, balance from account1;"

set -euo pipefail
TIMEFORMAT='%R %U %S'

DUCKDB_DB="$1"         # DuckDB database file path
QUERY="$2"             # SQL query or queries
LABEL="${3-}"          # Optional: label for output (e.g., relative path of query file)

# Function: prepend 0 if the number starts with a dot (e.g. .123456 -> 0.123456)
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
RESULTS_FILE="$RESULTS_DIR/duckdb_results.csv"

# Write CSV header if the file does not exist yet
if [[ ! -f "$RESULTS_FILE" ]]; then
  echo "dbms,label,iteration,execution_time,response_time" > "$RESULTS_FILE"
fi

echo "DUCKDB QUERY STARTED"
echo "DUCKDB database: $DUCKDB_DB"
if [[ -n "$LABEL" ]]; then
  echo "DUCKDB Query File: $LABEL"
else
  echo "DUCKDB Query: $QUERY"
fi
echo "Execution Time      Response Time"

for i in $(seq 1 11); do
  # Use built-in `time` to capture real/user/sys time
  { time duckdb "$DUCKDB_DB" -c "$QUERY" > /dev/null; } 2> duckdb_time_output.log

  # If DuckDB failed, print error and exit
  if [ $? -ne 0 ]; then
    echo "Error: DuckDB command failed."
    cat duckdb_time_output.log
    rm -f duckdb_time_output.log
    exit 1
  fi

  # Extract timing information
  TIME_LINE=$(grep -E '^[0-9.]+ +[0-9.]+ +[0-9.]+' duckdb_time_output.log | tail -n 1)
  if [ -z "$TIME_LINE" ]; then
    echo "Error: no timing information found in duckdb_time_output.log"
    rm -f duckdb_time_output.log
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

  echo "ran duckdb ${i}"
  echo "${EXECUTION_TIME}            ${REAL_TIME}"

  # Append result to CSV, dbms field is fixed to "duckdb"
  if [[ -n "$LABEL" ]]; then
    echo "duckdb,${LABEL},${i},${EXECUTION_TIME},${REAL_TIME}" >> "$RESULTS_FILE"
  else
    echo "duckdb,${QUERY},${i},${EXECUTION_TIME},${REAL_TIME}" >> "$RESULTS_FILE"
  fi

  rm -f duckdb_time_output.log
done

echo "DUCKDB QUERY DONE"
