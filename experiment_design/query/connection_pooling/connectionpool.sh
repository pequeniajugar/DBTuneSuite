#!/bin/bash
# Usage: ./pool.sh <database_name>
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <database_name>"
  exit 1
fi

DB_NAME="$1"

POOL_SIZES=(25 50 100)   # POOL_SIZE, and also MAX_CONN
THREADS_SET=(10 100 500) # NUM_THREADS
RUNS=11                  # 每个组合运行次数

echo "=== Running SIMPLE and POOL benchmarks on DB: ${DB_NAME} ==="

for PS in "${POOL_SIZES[@]}"; do
  for NT in "${THREADS_SET[@]}"; do
    for i in $(seq 1 $RUNS); do
      echo "Run #$i ----- SIMPLE: max_conn=${PS}, threads=${NT} -----"
      python3 simple.py --db "${DB_NAME}" --max-conn "${PS}" --threads "${NT}"

      echo "Run #$i ----- POOL: pool_size=${PS}, max_conn=${PS}, threads=${NT} -----"
      python3 pool.py --db "${DB_NAME}" --max-conn "${PS}" --pool-size "${PS}" --threads "${NT}"

      echo "---------------------------------------------------------------"
    done
  done
done

echo "=== All runs complete ==="
