#!/bin/bash
# Benchmark: Fraction scan window on PostgreSQL (big dataset, range)
# Output goes into a CSV file via base_postgres.sh with labels.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

database_name="employees_index"  # change if needed
output_csv="$SCRIPT_DIR/../../results/postgres_big_fraction_scan_win_range.csv"  # change if needed

mkdir -p "$(dirname "$output_csv")"
echo "dbms,label,iteration,execution_time,response_time" > "$output_csv"

PGUSER="username"       # should match base_postgres.sh
PGHOST="localhost"
PGPORT=5432

max_value=$(psql -U "$PGUSER" -h "$PGHOST" -p "$PGPORT" -d "$database_name" -t -A \
  -c "SELECT COALESCE(MAX(hundreds2), 0) FROM employee;" 2>/dev/null | tail -n 1)

if [[ -z "$max_value" || "$max_value" -le 100 ]]; then
  echo "Error: invalid max(hundreds2) value: $max_value"
  exit 1
fi

range=$((max_value - 100))

echo "Using thresholds (big, PostgreSQL) as percentages: 1,5,10,20,40"

for pct in 1 5 10 20 40; do
  X=$((range * pct / 100 + 100))
  label_suffix="${pct}pct"
  bash "$SCRIPT_DIR/../../base_postgres.sh" \
    "$database_name" \
    "SET enable_seqscan = off; SELECT * FROM employee WHERE hundreds2 < ${X};" \
    "non-clustered_${label_suffix}" \
    "$(basename "$output_csv")"

  bash "$SCRIPT_DIR/../../base_postgres.sh" \
    "$database_name" \
    "SELECT * FROM employee WHERE longitude < ${X};" \
    "noindex_${label_suffix}" \
    "$(basename "$output_csv")"
done

echo "PostgreSQL fraction-scan window (big, range) benchmark finished. Results saved to: $output_csv"
