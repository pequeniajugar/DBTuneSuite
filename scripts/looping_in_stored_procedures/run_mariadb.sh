#!/bin/bash
# Benchmark: Looping in Stored Procedures vs single SELECT on MariaDB
# Output goes into a CSV file via base_mariadb.sh with labels.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

database_name="tpch_small"  # change if needed
output_csv="$SCRIPT_DIR/../results/mariadb_looping_in_stored_procedures.csv"  # change if needed

# Ensure results directory exists
mkdir -p "$(dirname "$output_csv")"

# Recreate CSV header
echo "dbms,label,iteration,execution_time,response_time" > "$output_csv"

###############################################################################
# 1. Without loop: simple range predicate, varying threshold
###############################################################################

for i in $(seq 1 11); do
  bash "$SCRIPT_DIR/../base_mariadb.sh" \
    "$database_name" \
    "SELECT * FROM lineitem WHERE l_partkey < 200;" \
    "without loop" \
    "$(basename "$output_csv")"
done

###############################################################################
# 2. With loop: stored procedure that loops over l_partkey
###############################################################################

# Create (or replace) the stored procedure using configure_mariadb.sh
read -r -d '' PROC_SQL <<'SQL'
DROP PROCEDURE IF EXISTS get_lineitems;
DELIMITER //
CREATE PROCEDURE get_lineitems()
BEGIN
    DECLARE i INT DEFAULT 1;

    -- prepare statement for point lookups
    PREPARE stmt FROM 'SELECT * FROM lineitem WHERE l_partkey = ?';

    WHILE i < 200 DO
        SET @param = i;
        EXECUTE stmt USING @param;
        SET i = i + 1;
    END WHILE;

    -- release prepared statement
    DEALLOCATE PREPARE stmt;
END //
DELIMITER ;
SQL

bash "$SCRIPT_DIR/../configure_mariadb.sh" \
  "$database_name" \
  "$PROC_SQL"

# With loop: call the stored procedure 11 times, measure with base_mariadb.sh
for i in $(seq 1 11); do
  bash "$SCRIPT_DIR/../base_mariadb.sh" \
    "$database_name" \
    "CALL get_lineitems();" \
    "with loop" \
    "$(basename "$output_csv")"
done

