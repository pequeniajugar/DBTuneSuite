#!/bin/bash
# === pool.sh (MySQL benchmark runner) ===
# Usage: bash pool.sh <dbname>

set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <dbname>"
  exit 1
fi

# ==== Database connection parameters ====
DB_NAME="$1"             # database name passed from command line
DB_USER="root"           # edit manually
DB_PASS="pwd"            # edit manually
DB_HOST="localhost"      # edit manually
DB_PORT=15559            # edit manually
DB_SOCKET="/path/to/mysql.sock"  # edit manually

# ==== Experiment settings ====
POOL_SIZES=(25 50 100)   # also used for max_connections
THREADS_SET=(10 100 500) # number of worker threads
RUNS=11                  # number of repeated runs

echo "=== Running SIMPLE and POOL benchmarks on MySQL DB: ${DB_NAME} ==="

for PS in "${POOL_SIZES[@]}"; do
  for NT in "${THREADS_SET[@]}"; do
    for i in $(seq 1 $RUNS); do
      echo "Run #$i ----- SIMPLE: max_conn=${PS}, threads=${NT} -----"
      python3 simple.py \
        --db "${DB_NAME}" \
        --max-conn "${PS}" \
        --threads "${NT}" \
        --user "${DB_USER}" \
        --password "${DB_PASS}" \
        --host "${DB_HOST}" \
        --port "${DB_PORT}" \
        --socket "${DB_SOCKET}"

      echo "Run #$i ----- POOL: pool_size=${PS}, max_conn=${PS}, threads=${NT} -----"
      python3 pool.py \
        --db "${DB_NAME}" \
        --max-conn "${PS}" \
        --pool-size "${PS}" \
        --threads "${NT}" \
        --user "${DB_USER}" \
        --password "${DB_PASS}" \
        --host "${DB_HOST}" \
        --port "${DB_PORT}" \
        --socket "${DB_SOCKET}"

      echo "---------------------------------------------------------------"
    done
  done
done

echo "=== All runs complete ==="
