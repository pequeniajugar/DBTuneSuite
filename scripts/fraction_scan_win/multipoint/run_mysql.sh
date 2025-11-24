#!/bin/bash
# Benchmark: Multipoint queries with non-clustered index vs no index on MySQL
# Output goes into a CSV file via base_mysql.sh with labels.
#
# Table: scanwin_multipoint(onepercent1, onepercent2, fivepercent1, fivepercent2,
#                           tenpercent1, tenpercent2, twentypercent1, twentypercent2)
# For each access fraction (1%, 5%, 10%, 20%), we run multipoint equality queries
# on the indexed columns (non-clustered) and non-indexed columns (scan),
# cycling VALUE in [1,5] for each fraction.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

database_name="scanwin_multipoint"  # change if needed
output_csv="$SCRIPT_DIR/../../results/mysql_fraction_scan_win_multipoint.csv"  # change if needed

mkdir -p "$(dirname "$output_csv")"
echo "dbms,label,iteration,execution_time,response_time" > "$output_csv"

# Values for VALUE in [1,5]
values=(1 2 3 4 5)

run_fraction() {
  local column_indexed="$1"   # e.g., onepercent1
  local column_scan="$2"      # e.g., onepercent2
  local pct_label="$3"        # e.g., 1pct, 5pct

  for val in "${values[@]}"; do
    # Non-clustered index query
    bash "$SCRIPT_DIR/../../base_mysql.sh" \
      "$database_name" \
      "SELECT * FROM scanwin_multipoint WHERE ${column_indexed} = ${val};" \
      "non-clustered_${pct_label}" \
      "$(basename "$output_csv")"

    # Scan (no index) query
    bash "$SCRIPT_DIR/../../base_mysql.sh" \
      "$database_name" \
      "SELECT * FROM scanwin_multipoint WHERE ${column_scan} = ${val};" \
      "noindex_${pct_label}" \
      "$(basename "$output_csv")"
  done
}

# 1% access fraction
run_fraction "onepercent1" "onepercent2" "1pct"

# 5% access fraction
run_fraction "fivepercent1" "fivepercent2" "5pct"

# 10% access fraction
run_fraction "tenpercent1" "tenpercent2" "10pct"

# 20% access fraction
run_fraction "twentypercent1" "twentypercent2" "20pct"

echo "MySQL multipoint fraction-scan benchmark finished. Results saved to: $output_csv"

