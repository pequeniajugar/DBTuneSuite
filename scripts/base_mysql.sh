#!/bin/bash
# Run a single SQL query against MySQL via Unix socket, 11 repetitions with timing
# Usage: bash base_mysql.sh database_name "query with {v1} and/or {v2}" [optional_label] [optional_output_csv]

set -euo pipefail
TIMEFORMAT='%R %U %S'

# --- MySQL Connection Settings (via Unix Socket) ---
MYSQL_USER="root"     # Change to your MySQL username
MYSQL_PASSWORD=""     # Change to your MySQL password (if any)
MYSQL_SOCKET="/data/tw3090/mysql/mysql.sock"
MYSQL_BIN="/home/tw3090/mysql/mysql-commercial-9.1.0-linux-glibc2.28-x86_64/bin/mysql"
# ---------------------------------------------------

MYSQL_DB="$1"   # Database name
QUERY="$2"      # SQL query template
LABEL="${3-}"   # Optional label (e.g., query file name)
OUT_CSV="${4-}" # Optional output CSV file name

# --- Output preparation ---
RESULTS_DIR="./results"
mkdir -p "$RESULTS_DIR"
if [[ -n "$OUT_CSV" ]]; then
  RESULTS_FILE="$RESULTS_DIR/$OUT_CSV"
else
  RESULTS_FILE="$RESULTS_DIR/mysql_results.csv"
fi

# Write CSV header if file does not exist
if [[ ! -f "$RESULTS_FILE" ]]; then
  echo "dbms,label,iteration,execution_time,response_time" > "$RESULTS_FILE"
fi

# Helper: add a leading zero if number starts with a dot
pad0() {
  local val="$1"
  [[ "$val" =~ ^\.[0-9] ]] && echo "0$val" || echo "$val"
}

echo "MYSQL QUERY STARTED"
echo "MySQL database: $MYSQL_DB"
[[ -n "$LABEL" ]] && echo "Query File: $LABEL" || echo "Query Template: $QUERY"
echo "Execution Time      Response Time"

# --- Initial v1 value ---
value1=150

for i in $(seq 1 11); do
  value2=$((value1 + 1000))
  CURRENT_QUERY="${QUERY//\{v1\}/$value1}"
  CURRENT_QUERY="${CURRENT_QUERY//\{v2\}/$value2}"

  # Run query via Unix socket with timing
  { time "$MYSQL_BIN" \
    -u "$MYSQL_USER" \
    -S "$MYSQL_SOCKET" \
    "$MYSQL_DB" -e "$CURRENT_QUERY" > /dev/null; } 2> mysql_time_output.log

  # If MySQL fails, print error and exit
  if [[ $? -ne 0 ]]; then
    echo "Error: mysql command failed"
    cat mysql_time_output.log || true
    rm -f mysql_time_output.log
    exit 1
  fi

  # Extract timing info from the log
  TIME_LINE=$(grep -E '^[0-9.]+ +[0-9.]+ +[0-9.]+' mysql_time_output.log | tail -n 1 || true)
  if [[ -z "$TIME_LINE" ]]; then
    echo "Error: no timing info found"
    rm -f mysql_time_output.log
    exit 1
  fi

  read REAL_TIME USER_TIME SYS_TIME <<< "$TIME_LINE"
  REAL_TIME=$(pad0 "$(printf "%.6f" "$REAL_TIME")")
  USER_TIME=$(pad0 "$(printf "%.6f" "$USER_TIME")")
  SYS_TIME=$(pad0 "$(printf "%.6f" "$SYS_TIME")")
  EXECUTION_TIME=$(pad0 "$(echo "scale=6; $USER_TIME + $SYS_TIME" | bc)")

  echo "ran mysql ${i}"
  echo "${EXECUTION_TIME}            ${REAL_TIME}"

  # Append result row to CSV
  if [[ -n "$LABEL" ]]; then
    echo "mysql,${LABEL},${i},${EXECUTION_TIME},${REAL_TIME}" >> "$RESULTS_FILE"
  else
    echo "mysql,${CURRENT_QUERY},${i},${EXECUTION_TIME},${REAL_TIME}" >> "$RESULTS_FILE"
  fi

  rm -f mysql_time_output.log
  value1=$((value1 + 1))
done

echo "MYSQL QUERY DONE"
echo "Results saved to: $RESULTS_FILE"
