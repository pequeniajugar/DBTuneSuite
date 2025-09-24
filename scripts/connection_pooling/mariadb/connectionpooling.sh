#!/bin/bash
# === pool.sh ===
# Run benchmark experiments with SIMPLE (NullPool) and POOL (QueuePool) modes
# Usage: bash pool.sh <dbname>

set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <dbname>"
  exit 1
fi

# ==== Database connection parameters (edit here) ====
DB_NAME="$1"             # database name is passed from command line
DB_USER="username"       # set manually here
DB_PASS="pwd"            # set manually here
DB_HOST="localhost"      # set manually here
DB_PORT="15559"          # set manually here

# ==== Experiment settings ====
POOL_SIZES=(25 50 100)   # also used for max_connections
THREADS_SET=(10 100 500) # number of worker threads
RUNS=3                   # number of repeated runs

echo "=== Running SIMPLE and POOL benchmarks on DB: ${DB_NAME} ==="

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
        --port "${DB_PORT}"

      echo "Run #$i ----- POOL: pool_size=${PS}, max_conn=${PS}, threads=${NT} -----"
      python3 pool.py \
        --db "${DB_NAME}" \
        --max-conn "${PS}" \
        --pool-size "${PS}" \
        --threads "${NT}" \
        --user "${DB_USER}" \
        --password "${DB_PASS}" \
        --host "${DB_HOST}" \
        --port "${DB_PORT}"

      echo "---------------------------------------------------------------"
    done
  done
done

echo "=== All runs complete ==="
