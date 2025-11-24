#!/bin/bash
# Benchmark: Denormalization on PostgreSQL (big TPCH) using base_postgres.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

database_name="tpch_big"  # change if needed
output_csv="$SCRIPT_DIR/../results/postgres_big_denormalization.csv"  # change if needed

# Ensure results directory exists
mkdir -p "$(dirname "$output_csv")"

# Recreate CSV header
echo "dbms,label,iteration,execution_time,response_time" > "$output_csv"

# Without Denormalization (join over normalized tables)
bash "$SCRIPT_DIR/../base_postgres.sh" \
  "$database_name" \
  "SELECT L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT, R_NAME FROM lineitem AS L, supplier AS S, nation AS N, region AS R WHERE L.L_SUPPKEY = S.S_SUPPKEY AND S.S_NATIONKEY = N.N_NATIONKEY AND N.N_REGIONKEY = R.R_REGIONKEY AND R.R_NAME = ’EUROPE’;" \
  "Without Denormalization" \
  "$(basename "$output_csv")"

# With Denormalization (create denormalized table if not exists, then query)
bash "$SCRIPT_DIR/../configure_postgres.sh" \
  "$database_name" \
  "CREATE TABLE IF NOT EXISTS lineitemdenormalized AS
   SELECT L.L_ORDERKEY, L.L_PARTKEY, L.L_SUPPKEY, L.L_LINENUMBER, L.L_QUANTITY, L.L_EXTENDEDPRICE, L.L_DISCOUNT, L.L_TAX, L.L_RETURNFLAG, L.L_LINESTATUS, L.L_SHIPDATE, L.L_COMMITDATE, L.L_RECEIPTDATE, L.L_SHIPINSTRUCT, L.L_SHIPMODE, L.L_COMMENT, R.R_NAME AS R_REGION
   FROM lineitem L
   JOIN supplier S ON L.L_SUPPKEY = S.S_SUPPKEY
   JOIN nation N ON S.S_NATIONKEY = N.N_NATIONKEY
   JOIN region R ON N.N_REGIONKEY = R.R_REGIONKEY;"

bash "$SCRIPT_DIR/../base_postgres.sh" \
  "$database_name" \
  "SELECT L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT, R_REGION FROM lineitemdenormalized WHERE R_REGION = ’EUROPE’;" \
  "With Denormalization" \
  "$(basename "$output_csv")"

# bash "$SCRIPT_DIR/../configure_postgres.sh" \
#   "$database_name" \
#   "DROP TABLE lineitemdenormalized;"
