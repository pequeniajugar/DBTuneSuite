#!/bin/bash
# Usage: ./run_duckdb_big.sh /path/to/your.duckdb
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUTPUT_FILE="$SCRIPT_DIR/../results/duckdb_big_vp_point.txt"
OUTPUT_LOG="$SCRIPT_DIR/duckdb_output.txt"
mkdir -p "$SCRIPT_DIR/../results"

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <duckdb_db_file>"
  exit 1
fi
DB_PATH="$1"

fractions=("n0" "n20" "n40" "n60" "n80" "n100")

# initialize output files
echo "===== DuckDB vertical_partition_point results =====" > "$OUTPUT_FILE"
echo "DB: $DB_PATH" >> "$OUTPUT_FILE"
echo "" >> "$OUTPUT_FILE"

echo "===== DuckDB per-query timings =====" > "$OUTPUT_LOG"
echo "DB: $DB_PATH" >> "$OUTPUT_LOG"
echo "" >> "$OUTPUT_LOG"

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
      bash "$SCRIPT_DIR/../base_duckdb.sh" "$DB_PATH" "$(cat "$QUERY_FILE")" "$REL_PATH"
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
      bash "$SCRIPT_DIR/../base_duckdb.sh" "$DB_PATH" "$(cat "$QUERY_FILE")" "$REL_PATH"
      echo ""
    } | tee -a "$OUTPUT_FILE" >> "$OUTPUT_LOG"
  else
    echo "Warning: Missing query file $QUERY_FILE" >&2
  fi
done

{
  echo "===== DONE ====="
} | tee -a "$OUTPUT_FILE" >> "$OUTPUT_LOG"
