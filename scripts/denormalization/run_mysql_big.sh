#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
output_file="$SCRIPT_DIR/../results/mysql_big_denormalization.txt"
database_name="tpch_big"      #change name

echo "-----Without Denormalization-----" > "$output_file"
bash ../base_mysql.sh "$database_name" "SELECT L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT, R_NAME FROM lineitem AS L, supplier AS S, nation AS N, region AS R WHERE L.L_SUPPKEY = S.S_SUPPKEY AND S.S_NATIONKEY = N.N_NATIONKEY AND N.N_REGIONKEY = R.R_REGIONKEY AND R.R_NAME = ’EUROPE’;" >> "$output_file"

echo "-----With Denormalization-----" > "$output_file"
bash ../configure_mysql.sh "$database_name" "CREATE TABLE lineitemdenormalized AS SELECT L.L_ORDERKEY, L.L_PARTKEY, L.L_SUPPKEY, L.L_LINENUMBER, L.L_QUANTITY, L.L_EXTENDEDPRICE, L.L_DISCOUNT, L.L_TAX, L.L_RETURNFLAG, L.L_LINESTATUS, L.L_SHIPDATE, L.L_COMMITDATE, L.L_RECEIPTDATE, L.L_SHIPINSTRUCT, L.L_SHIPMODE, L.L_COMMENT, R.R_NAME AS R_REGION FROM lineitem L JOIN supplier S ON L.L_SUPPKEY = S.S_SUPPKEY JOIN nation N ON S.S_NATIONKEY = N.N_NATIONKEY JOIN region R ON N.N_REGIONKEY = R.R_REGIONKEY;"
bash ../base_mysql.sh "$database_name" "SELECT L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT, R_REGION FROM lineitemdenormalized WHERE R_REGION = ’EUROPE’;" >> "$output_file"
bash ../configure_mysql.sh "$database_name" "DROP TABLE lineitemdenormalized;"
