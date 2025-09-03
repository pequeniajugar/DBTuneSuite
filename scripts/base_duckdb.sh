#!/bin/bash
# Run a single SQL query against DuckDB, 11 repetitions with timing
# usage: bash base_duckdb.sh database_path "query()ies"
#e.g. bash base_duckdb.sh /data/username/duckdb/account_new_7 "SELECT * FROM account LIMIT 10;"

set -euo pipefail

DUCKDB_DB="$1"         # DuckDB file path
QUERY="$2"             # query(ies)
LABEL="${3-}"          # Optional: Label for display (relative path)

pad0() {
  # if looks like .123456 then fill the missing 0 to 0.123456
  local val="$1"
  if [[ "$val" =~ ^\.[0-9] ]]; then
    echo "0$val"
  else
    echo "$val"
  fi
}

echo "DUCKDB QUERY STARTED"
echo "DUCKDB database: $DUCKDB_DB"
if [[ -n "$LABEL" ]]; then
  echo "DUCKDB Query File: $LABEL"
else
  echo "DUCKDB Query: $QUERY"
fi
echo "Execution Time      Response Time"

for i in $(seq 1 11); do
  # use /usr/bin/time record real/user/sys
  /usr/bin/time -f "%e %U %S" -o duckdb_time_output.log \
    duckdb "$DUCKDB_DB" -c "$QUERY" > /dev/null

  if [ $? -ne 0 ]; then
    echo "Error: DuckDB command failed."
    cat duckdb_time_output.log
    rm -f duckdb_time_output.log
    exit 1
  fi

  TIME_LINE=$(tail -n 1 duckdb_time_output.log)
  read REAL_TIME USER_TIME SYS_TIME <<< "$TIME_LINE"

 
  REAL_TIME=$(printf "%.6f" "$REAL_TIME")
  USER_TIME=$(printf "%.6f" "$USER_TIME")
  SYS_TIME=$(printf "%.6f" "$SYS_TIME")

  # calculate execution time（user+sys）
  EXECUTION_TIME=$(echo "scale=6; $USER_TIME + $SYS_TIME" | bc)

  # fill in the missing 0
  REAL_TIME=$(pad0 "$REAL_TIME")
  EXECUTION_TIME=$(pad0 "$EXECUTION_TIME")

  echo "ran duckdb ${i}"
  echo "${EXECUTION_TIME}            ${REAL_TIME}"

  rm -f duckdb_time_output.log
done

echo "DUCKDB QUERY DONE"
