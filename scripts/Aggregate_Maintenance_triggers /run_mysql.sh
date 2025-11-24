#!/bin/bash
# Benchmark: Aggregate Maintenance with and without Triggers on MySQL
# - With triggers: use AFTER INSERT triggers on orders to maintain vendorOutstanding/storeOutstanding.
# - Without triggers: compute aggregates via explicit SUM(...) queries.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

database_name="store_new_10_5"  # change if needed
tbl_file_path="/data/tw3090/store/triggers_input.csv"  # change if needed

# CSV for query timings (all base_mysql.sh runs)
query_output_csv="$SCRIPT_DIR/../results/mysql_aggregate_triggers_queries.csv"  # change if needed

# Ensure results directory exists for this path
mkdir -p "$(dirname "$query_output_csv")"
# Start fresh so base_mysql.sh writes the header once
rm -f "$query_output_csv"

# --- Capture initial max ordernum before all experiments (for restoration) ---
MYSQL_USER="root"        # keep in sync with scripts/configure_mysql.sh
MYSQL_PASSWORD="pwd"
MYSQL_HOST="localhost"
MYSQL_PORT=3306          # change if your MySQL is on a different port

initial_max_ordernum=$(
  mysql -u "$MYSQL_USER" -p"$MYSQL_PASSWORD" \
    -h "$MYSQL_HOST" -P "$MYSQL_PORT" -N -D "$database_name" \
    -e "SELECT COALESCE(MAX(ordernum), 0) FROM orders;" \
  || echo 0
)

echo "=== Aggregate Maintenance experiment on MySQL ==="

###############################################################################
# 1. WITH TRIGGERS
###############################################################################

echo ">>> Setting up triggers (WITH triggers)..."

# NOTE: CREATE INDEX IF NOT EXISTS requires MySQL 8.0+; if you're on an older
#       version, remove 'IF NOT EXISTS' and create the index manually once.
read -r -d '' TRIGGER_SQL <<'SQL'
CREATE INDEX IF NOT EXISTS i_item ON item(itemnum);

DELIMITER $$

DROP TRIGGER IF EXISTS updateVendorOutstanding $$
CREATE TRIGGER updateVendorOutstanding
AFTER INSERT ON orders
FOR EACH ROW
BEGIN
    UPDATE vendorOutstanding
    SET amount = amount + (NEW.quantity * (
        SELECT price
        FROM item
        WHERE item.itemnum = NEW.itemnum
    ))
    WHERE vendorid = NEW.vendorid;
END $$

DROP TRIGGER IF EXISTS updateStoreOutstanding $$
CREATE TRIGGER updateStoreOutstanding
AFTER INSERT ON orders
FOR EACH ROW
BEGIN
    UPDATE storeOutstanding
    SET amount = amount + (NEW.quantity * (
        SELECT price
        FROM item
        WHERE item.itemnum = NEW.itemnum
    ))
    WHERE storeid = NEW.storeid;
END $$

DELIMITER ;
SQL

bash "$SCRIPT_DIR/../../configure_mysql.sh" \
  "$database_name" \
  "$TRIGGER_SQL"

echo ">>> Inserting data row-by-row with triggers (11 runs, times recorded by trigger_data_insert.py)..."

python "$SCRIPT_DIR/trigger_data_insert.py" \
  --mode with_trigger \
  --file "$tbl_file_path"

echo ">>> Measuring query performance WITH triggers using base_mysql.sh..."

# Vendor outstanding lookups via maintained aggregate table
vendor_id=10
for i in $(seq 1 11); do
  bash "$SCRIPT_DIR/../../base_mysql.sh" \
    "$database_name" \
    "SELECT amount FROM vendorOutstanding WHERE vendorid = '${vendor_id}';" \
    "with_trigger_vendor" \
    "$(basename "$query_output_csv")"
  vendor_id=$((vendor_id + 1))
done

# Store outstanding lookups via maintained aggregate table
store_id=10
for i in $(seq 1 11); do
  bash "$SCRIPT_DIR/../../base_mysql.sh" \
    "$database_name" \
    "SELECT amount FROM storeOutstanding WHERE storeid = '${store_id}';" \
    "with_trigger_store" \
    "$(basename "$query_output_csv")"
  store_id=$((store_id + 1))
done

echo ">>> Restoring tables to pre-insertion state after WITH-triggers experiment..."

bash "$SCRIPT_DIR/../../configure_mysql.sh" \
  "$database_name" \
  "DELETE FROM orders WHERE ordernum > ${initial_max_ordernum};"

###############################################################################
# 2. WITHOUT TRIGGERS
###############################################################################

echo ">>> Dropping triggers (WITHOUT triggers)..."

read -r -d '' DROP_TRIGGERS_SQL <<'SQL'
DELIMITER $$
DROP TRIGGER IF EXISTS updateVendorOutstanding $$
DROP TRIGGER IF EXISTS updateStoreOutstanding $$
DELIMITER ;
SQL

bash "$SCRIPT_DIR/../../configure_mysql.sh" \
  "$database_name" \
  "$DROP_TRIGGERS_SQL"

echo ">>> Inserting data row-by-row WITHOUT triggers (11 runs, times recorded by trigger_data_insert.py)..."

python "$SCRIPT_DIR/trigger_data_insert.py" \
  --mode without_trigger \
  --file "$tbl_file_path"

echo ">>> Measuring query performance WITHOUT triggers using base_mysql.sh..."

# Vendor aggregate using explicit join and SUM
vendor_id=10
for i in $(seq 1 11); do
  bash "$SCRIPT_DIR/../../base_mysql.sh" \
    "$database_name" \
    "SELECT SUM(orders.quantity * item.price)
     FROM orders, item
     WHERE orders.itemnum = item.itemnum
       AND orders.vendorid = '${vendor_id}';" \
    "without_trigger_vendor" \
    "$(basename "$query_output_csv")"
  vendor_id=$((vendor_id + 1))
done

# Store aggregate using explicit join and SUM
store_id=10
for i in $(seq 1 11); do
  bash "$SCRIPT_DIR/../../base_mysql.sh" \
    "$database_name" \
    "SELECT SUM(orders.quantity * item.price)
     FROM orders, item, store
     WHERE orders.itemnum = item.itemnum
       AND orders.storeid = store.storeid
       AND store.storeid = '${store_id}';" \
    "without_trigger_store" \
    "$(basename "$query_output_csv")"
  store_id=$((store_id + 1))
done

echo ">>> Restoring orders table to pre-insertion state after WITHOUT-triggers experiment..."

bash "$SCRIPT_DIR/../../configure_mysql.sh" \
  "$database_name" \
  "DELETE FROM orders WHERE ordernum > ${initial_max_ordernum};"

echo "Aggregate Maintenance (triggers vs no triggers) experiment finished."
