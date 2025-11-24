#!/bin/bash
# Benchmark: Cursor vs no cursor on MariaDB
# Output goes into a CSV file via base_mariadb.sh with labels

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

database_name="employees"  # change if needed
output_csv="$SCRIPT_DIR/../results/mariadb_cursor.csv"  # change if needed

# Ensure results directory exists
mkdir -p "$(dirname "$output_csv")"

# Recreate CSV header
echo "dbms,label,iteration,execution_time,response_time" > "$output_csv"

# --- Without cursor: simple full table scan ---
bash "$SCRIPT_DIR/../base_mariadb.sh" \
  "$database_name" \
  "SELECT * FROM employees;" \
  "without cursor" \
  "$(basename "$output_csv")"

# --- With cursor: create stored procedure + CALL ---

# Create (or replace) the cursor-based stored procedure using configure_mariadb.sh
read -r -d '' PROC_SQL <<'SQL'
DROP PROCEDURE IF EXISTS fetch_employees;
DELIMITER //
CREATE PROCEDURE fetch_employees()
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE emp_ssnum INT;
    DECLARE emp_name VARCHAR(255);
    DECLARE emp_lat DECIMAL(10,2);
    DECLARE emp_longitude DECIMAL(10,2);
    DECLARE emp_hundreds1 INT;
    DECLARE emp_hundreds2 INT;

    -- Declare cursor
    DECLARE emp_cursor CURSOR FOR 
        SELECT ssnum, name, lat, longitude, hundreds1, hundreds2 FROM employees;

    -- Finish dealing with cursor
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    -- Open cursor
    OPEN emp_cursor;

    -- Read cursor
    read_loop: LOOP
        FETCH emp_cursor INTO emp_ssnum, emp_name, emp_lat, emp_longitude, emp_hundreds1, emp_hundreds2;
        IF done THEN
            LEAVE read_loop;
        END IF;

        -- Print line by line
        SELECT emp_ssnum AS SSN,
               emp_name AS Name,
               emp_lat AS Latitude,
               emp_longitude AS Longitude,
               emp_hundreds1,
               emp_hundreds2;
    END LOOP;

    -- Close cursor
    CLOSE emp_cursor;
END//
DELIMITER ;
SQL

bash "$SCRIPT_DIR/../configure_mariadb.sh" \
  "$database_name" \
  "$PROC_SQL"

# With cursor: call the stored procedure
bash "$SCRIPT_DIR/../base_mariadb.sh" \
  "$database_name" \
  "CALL fetch_employees();" \
  "with cursor" \
  "$(basename "$output_csv")"

