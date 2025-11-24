#!/bin/bash
# Benchmark: Multipoint queries with non-clustered index vs no index on MariaDB
# Output goes into a CSV file via base_mariadb.sh with labels.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

database_name="scanwin_multipoint"  # change if needed
output_csv="$SCRIPT_DIR/../../results/mariadb_fraction_scan_win_multipoint.csv"  # change if needed

mkdir -p "$(dirname "$output_csv")"
echo "dbms,label,iteration,execution_time,response_time" > "$output_csv"

values=(1 2 3 4 5)

run_fraction() {
  local column_indexed="$1"
  local column_scan="$2"
  local pct_label="$3"

  for val in "${values[@]}"; do
    # Non-clustered index query
    bash "$SCRIPT_DIR/../../base_mariadb.sh" \
      "$database_name" \
      "SELECT * FROM scanwin_multipoint WHERE ${column_indexed} = ${val};" \
      "non-clustered_${pct_label}" \
      "$(basename "$output_csv")"

    # Scan query (no index)
    bash "$SCRIPT_DIR/../../base_mariadb.sh" \
      "$database_name" \
      "SELECT * FROM scanwin_multipoint WHERE ${column_scan} = ${val};" \
      "noindex_${pct_label}" \
      "$(basename "$output_csv")"
  done
}

run_fraction "onepercent1" "onepercent2" "1pct"
run_fraction "fivepercent1" "fivepercent2" "5pct"
run_fraction "tenpercent1" "tenpercent2" "10pct"
run_fraction "twentypercent1" "twentypercent2" "20pct"

echo "MariaDB multipoint fraction-scan benchmark finished. Results saved to: $output_csv"

