#!/bin/bash
# Usage: ./run_mariadb_big.sh <mariadb_database_name>
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUTPUT_FILE="$SCRIPT_DIR/../results/mariadb_big_vp_point.txt"
OUTPUT_LOG="$SCRIPT_DIR/mariadb_output.txt"
mkdir -p "$SCRIPT_DIR/../results"

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <mariadb_database_name>"
  exit 1
fi
DB_NAME="$1"

fractions=("n0" "n20" "n40" "n60" "n80" "n100")

# initialize output files
{
  echo "===== MariaDB vertical_partition_point results ====="
  echo "DB: $DB_NAME"
  echo ""
} > "$OUTPUT_FILE"

{
  echo "===== MariaDB per-query timings ====="
  echo "DB: $DB_NAME"
  echo ""
} > "$OUTPUT_LOG"

### Without VP
{
  echo "----- Without Vertical Partitioning -----"
} | tee -a "$OUTPUT_FILE" >> "$OUTPUT_LOG"

for frac in "${fractions[@]}"; do
  QUERY_FILE="$SCRIPT_DIR/queries/without_vp/${frac}.txt"
  if [[ -f "$QUERY_FILE" ]]; then
    REL_PATH="${QUERY_FILE#$SCRIPT_DIR/}"
    {
      echo "--- Access Fraction $frac ---"
      bash "$SCRIPT_DIR/../base_mariadb.sh" "$DB_NAME" "$(cat "$QUERY_FILE")" "$REL_PATH"
      echo ""
    } | tee -a "$OUTPUT_FILE" >> "$OUTPUT_LOG"
  else
    echo "Warning: Missing query file $QUERY_FILE" >&2
  fi
done

### With VP
{
  echo "----- With Vertical Partitioning -----"
} | tee -a "$OUTPUT_FILE" >> "$OUTPUT_LOG"

for frac in "${fractions[@]}"; do
  QUERY_FILE="$SCRIPT_DIR/queries/with_vp/${frac}.txt"
  if [[ -f "$QUERY_FILE" ]]; then
    REL_PATH="${QUERY_FILE#$SCRIPT_DIR/}"
    {
      echo "--- Access Fraction $frac ---"
      bash "$SCRIPT_DIR/../base_mariadb.sh" "$DB_NAME" "$(cat "$QUERY_FILE")" "$REL_PATH"
      echo ""
    } | tee -a "$OUTPUT_FILE" >> "$OUTPUT_LOG"
  else
    echo "Warning: Missing query file $QUERY_FILE" >&2
  fi
done

{
  echo "===== DONE ====="
} | tee -a "$OUTPUT_FILE" >> "$OUTPUT_LOG"
