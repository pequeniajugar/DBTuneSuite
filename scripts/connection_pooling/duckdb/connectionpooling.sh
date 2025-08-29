#!/bin/bash
# Usage: ./connectionpooling.sh <duckdb_file_address>
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <duckdb_file>"
  exit 1
fi

DB_FILE="$1"

POOL_SIZES=(25 50 100)  #POOL SIZES
THREADS_SET=(10 100 500) # NUM_THREADs
RUNS=11                  # NUMBER OF RUNS OF EACH COMBINATION

echo "=== Running SIMPLE and POOL benchmarks on DuckDB: ${DB_FILE} ==="

for PS in "${POOL_SIZES[@]}"; do
  for NT in "${THREADS_SET[@]}"; do
    for i in $(seq 1 $RUNS); do
      echo "Run #$i ----- SIMPLE: threads=${NT} -----"
      python3 simple.py --db "${DB_FILE}" --threads "${NT}"

      echo "Run #$i ----- POOL: pool_size=${PS}, threads=${NT} -----"
      python3 pool.py --db "${DB_FILE}" --pool-size "${PS}" --threads "${NT}"

      echo "---------------------------------------------------------------"
    done
  done
done

echo "=== All runs complete ==="
