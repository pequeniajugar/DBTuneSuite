#!/bin/bash
# Benchmark: Fraction scan window on MariaDB (big dataset, range)
# Output goes into a CSV file via base_mariadb.sh with labels.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

database_name="employees_index"  # change if needed
output_csv="$SCRIPT_DIR/../../results/mariadb_big_fraction_scan_win_range.csv"  # change if needed

mkdir -p "$(dirname "$output_csv")"
echo "dbms,label,iteration,execution_time,response_time" > "$output_csv"

MYSQL_USER="username"    # should match base_mariadb.sh
MYSQL_PASSWORD="pwd"
MYSQL_HOST="localhost"
MYSQL_PORT=15559

max_value=$(mysql -u "$MYSQL_USER" -p"$MYSQL_PASSWORD" \
  -h "$MYSQL_HOST" -P "$MYSQL_PORT" -N -D "$database_name" \
  -e "SELECT COALESCE(MAX(hundreds2), 0) FROM employee;" | tail -n 1)

if [[ -z "$max_value" || "$max_value" -le 100 ]]; then
  echo "Error: invalid max(hundreds2) value: $max_value"
  exit 1
fi

range=$((max_value - 100))

echo "Using thresholds (big, MariaDB) as percentages: 1,5,10,20,40"

for pct in 1 5 10 20 40; do
  X=$((range * pct / 100 + 100))
  label_suffix="${pct}pct"
  bash "$SCRIPT_DIR/../../base_mariadb.sh" \
    "$database_name" \
    "SELECT * FROM employee WHERE hundreds2 < ${X};" \
    "non-clustered_${label_suffix}" \
    "$(basename "$output_csv")"

  bash "$SCRIPT_DIR/../../base_mariadb.sh" \
    "$database_name" \
    "SELECT * FROM employee WHERE longitude < ${X};" \
    "noindex_${label_suffix}" \
    "$(basename "$output_csv")"
done

echo "MariaDB fraction-scan window (big, range) benchmark finished. Results saved to: $output_csv"
