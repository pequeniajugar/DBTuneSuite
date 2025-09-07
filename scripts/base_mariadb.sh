#!/bin/bash
# Run a single SQL query against MariaDB, 11 repetitions with timing (built-in `time`)
# usage:  bash base_mariadb.sh database_name "query(ies with {v1} and/or {v2})" [optional_label]
# example: bash base_mariadb.sh account_new_7 "SELECT * FROM account1 WHERE hundreds1 BETWEEN {v1} AND {v2};" label1

set -euo pipefail
TIMEFORMAT='%R %U %S'

# --- Connection settings ---
MYSQL_USER="tw3090"
MYSQL_PASSWORD="64113491Ka"
MYSQL_HOST="localhost"
MYSQL_PORT=15559
# ----------------------------

MYSQL_DB="$1"         # MariaDB database name
QUERY="$2"            # SQL query template (may include {v1}, {v2})
LABEL="${3-}"         # Optional label for CSV output

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

# Write CSV header if not exists
if [[ ! -f "$RESULTS_FILE" ]]; then
  echo "dbms,label,iteration,execution_time,response_time" > "$RESULTS_FILE"
fi

echo "MARIADB QUERY STARTED"
echo "MariaDB database: $MYSQL_DB"
if [[ -n "$LABEL" ]]; then
  echo "Query File: $LABEL"
else
  echo "Query Template: $QUERY"
fi
echo "Execution Time      Response Time"

# --- Initial value for v1 ---
value1=800

for i in $(seq 1 11); do
  value2=$((value1 + 1000))

  # Replace placeholders {v1} and {v2}
  CURRENT_QUERY="${QUERY//\{v1\}/$value1}"
  CURRENT_QUERY="${CURRENT_QUERY//\{v2\}/$value2}"

  # Run query and time it
  { time mysql \
      -u "$MYSQL_USER" -p"$MYSQL_PASSWORD" \
      -h "$MYSQL_HOST" -P "$MYSQL_PORT" \
      "$MYSQL_DB" -e "$CURRENT_QUERY" > /dev/null; } 2> mariadb_time_output.log

  # Check if failed
  if [ $? -ne 0 ]; then
    echo "Error: mysql command failed."
    cat mariadb_time_output.log || true
    rm -f mariadb_time_output.log
    exit 1
  fi

  # Parse timing
  TIME_LINE=$(grep -E '^[0-9.]+ +[0-9.]+ +[0-9.]+' mariadb_time_output.log | tail -n 1 || true)
  if [ -z "$TIME_LINE" ]; then
    echo "Error: no timing info found in mariadb_time_output.log"
    rm -f mariadb_time_output.log
    exit 1
  fi

  read REAL_TIME USER_TIME SYS_TIME <<< "$TIME_LINE"

  REAL_TIME=$(printf "%.6f" "$REAL_TIME")
  USER_TIME=$(printf "%.6f" "$USER_TIME")
  SYS_TIME=$(printf "%.6f" "$SYS_TIME")
  EXECUTION_TIME=$(echo "scale=6; $USER_TIME + $SYS_TIME" | bc)

  REAL_TIME=$(pad0 "$REAL_TIME")
  EXECUTION_TIME=$(pad0 "$EXECUTION_TIME")

  echo "ran mariadb ${i} (v1=$value1, v2=$value2)"
  echo "${EXECUTION_TIME}            ${REAL_TIME}"

  if [[ -n "$LABEL" ]]; then
    echo "mariadb,${LABEL},${i},${EXECUTION_TIME},${REAL_TIME}" >> "$RESULTS_FILE"
  else
    echo "mariadb,${CURRENT_QUERY},${i},${EXECUTION_TIME},${REAL_TIME}" >> "$RESULTS_FILE"
  fi

  rm -f mariadb_time_output.log

  # Increment values
  value1=$((value1 + 1))
done

echo "MARIADB QUERY DONE"
