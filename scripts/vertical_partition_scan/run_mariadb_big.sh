#!/bin/bash
# Usage: ./run_mariadb_big.sh <database_name>
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULTS_DIR="$SCRIPT_DIR/../results"
mkdir -p "$RESULTS_DIR"
OUTPUT_FILE="$RESULTS_DIR/mariadb_big_vp_scan.txt"

# Check user input
if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <database_name>"
  exit 1
fi
DATABASE_NAME="$1"

# --- Extreme Cases ---
echo "-----extreme cases-----" > "$OUTPUT_FILE"

echo "---with vertical partitioning---" >> "$OUTPUT_FILE"
bash "$SCRIPT_DIR/../base_mariadb.sh" "$DATABASE_NAME" "SELECT id, balance FROM account1;" >> "$OUTPUT_FILE"

echo "---without vertical partitioning---" >> "$OUTPUT_FILE"
bash "$SCRIPT_DIR/../base_mariadb.sh" "$DATABASE_NAME" "SELECT id, balance FROM account;" >> "$OUTPUT_FILE"


# --- Without Vertical Partitioning ---
echo "-----Without Vertical Partitioning-----" >> "$OUTPUT_FILE"

echo "---Access All Fields---" >> "$OUTPUT_FILE"
bash "$SCRIPT_DIR/../base_mariadb.sh" "$DATABASE_NAME" "SELECT * FROM account;" >> "$OUTPUT_FILE"

echo "---Access Partial Fields---" >> "$OUTPUT_FILE"
bash "$SCRIPT_DIR/../base_mariadb.sh" "$DATABASE_NAME" "SELECT id, balance FROM account;" >> "$OUTPUT_FILE"


# --- With Vertical Partitioning ---
echo "-----With Vertical Partitioning-----" >> "$OUTPUT_FILE"

echo "---Access All Fields---" >> "$OUTPUT_FILE"
bash "$SCRIPT_DIR/../base_mariadb.sh" "$DATABASE_NAME" \
"SELECT account1.id, balance, homeaddress FROM account1, account2 WHERE account1.id = account2.id;" >> "$OUTPUT_FILE"

echo "---Access Partial Fields---" >> "$OUTPUT_FILE"
bash "$SCRIPT_DIR/../base_mariadb.sh" "$DATABASE_NAME" "SELECT id, balance FROM account1;" >> "$OUTPUT_FILE"
