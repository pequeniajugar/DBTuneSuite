#!/bin/bash
# Run a single SQL query against MariaDB (single execution, not loop)
# Usage:   bash base_mariadb_simple.sh database_name "query"
# Example: bash base_mariadb_simple.sh account_new_7 "ALTER TABLE employees ENGINE = MEMORY;"

set -euo pipefail
TIMEFORMAT='%R %U %S'

# --- MariaDB Connection settings ---
MYSQL_USER="root"            # change if needed
MYSQL_PASSWORD="pwd"         # change if needed
MYSQL_HOST="localhost"
MYSQL_PORT=15559             # MariaDB default may differ
# -----------------------------------

MYSQL_DB="$1"   # MariaDB database name
QUERY="$2"      # SQL query to run

echo "MARIADB QUERY STARTED"
echo "MariaDB database: $MYSQL_DB"
echo "Query: $QUERY"

# Run query once with error handling
{ time mysql \
    -u "$MYSQL_USER" -p"$MYSQL_PASSWORD" \
    -h "$MYSQL_HOST" -P "$MYSQL_PORT" \
    "$MYSQL_DB" -e "$QUERY"; } 2> mariadb_time_output.log

if [ $? -ne 0 ]; then
  echo "Error: Connection or query execution failed on MariaDB."
  echo "Details:"
  cat mariadb_time_output.log || true
  rm -f mariadb_time_output.log
  exit 1
fi

echo "MARIADB QUERY DONE"
rm -f mariadb_time_output.log
