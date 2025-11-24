#!/bin/bash
# Benchmark: Aggregate Maintenance with and without Triggers on PostgreSQL
# - With triggers: use AFTER INSERT triggers on orders to maintain vendorOutstanding/storeOutstanding.
# - Without triggers: compute aggregates via explicit SUM(...) queries.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

database_name="store_new_10_5"  # change if needed
tbl_file_path="/data/tw3090/store/triggers_input.csv"  # change if needed

# CSV for query timings (all base_postgres.sh runs)
query_output_csv="$SCRIPT_DIR/../results/postgres_aggregate_triggers_queries.csv"  # change if needed

# Ensure results directory exists for this path
mkdir -p "$(dirname "$query_output_csv")"
# Start fresh so base_postgres.sh writes the header once
rm -f "$query_output_csv"

# --- Capture initial max ordernum before all experiments (for restoration) ---
PG_USER="postgres"      # change if needed
PG_PASSWORD="pwd"       # change if needed
PG_HOST="localhost"
PG_PORT=5432            # change if your Postgres is on a different port

initial_max_ordernum=$(
  PGPASSWORD="$PG_PASSWORD" psql \
    -U "$PG_USER" \
    -h "$PG_HOST" \
    -p "$PG_PORT" \
    -d "$database_name" \
    -t -A \
    -c "SELECT COALESCE(MAX(ordernum), 0) FROM orders;" \
  2>/dev/null || echo 0
)

echo "=== Aggregate Maintenance experiment on PostgreSQL ==="

###############################################################################
# 1. WITH TRIGGERS
###############################################################################

echo ">>> Setting up triggers (WITH triggers)..."

# NOTE: CREATE INDEX IF NOT EXISTS is supported on PostgreSQL 9.5+.
read -r -d '' TRIGGER_SQL <<'SQL'
CREATE INDEX IF NOT EXISTS i_item ON item(itemnum);

-- Function to maintain vendorOutstanding
CREATE OR REPLACE FUNCTION update_vendor_outstanding()
RETURNS trigger AS $$
BEGIN
    UPDATE vendorOutstanding
    SET amount = amount + (NEW.quantity * (
        SELECT price
        FROM item
        WHERE item.itemnum = NEW.itemnum
    ))
    WHERE vendorid = NEW.vendorid;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Function to maintain storeOutstanding
CREATE OR REPLACE FUNCTION update_store_outstanding()
RETURNS trigger AS $$
BEGIN
    UPDATE storeOutstanding
    SET amount = amount + (NEW.quantity * (
        SELECT price
        FROM item
        WHERE item.itemnum = NEW.itemnum
    ))
    WHERE storeid = NEW.storeid;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Drop and recreate triggers on orders
DROP TRIGGER IF EXISTS updateVendorOutstanding ON orders;
CREATE TRIGGER updateVendorOutstanding
AFTER INSERT ON orders
FOR EACH ROW
EXECUTE FUNCTION update_vendor_outstanding();

DROP TRIGGER IF EXISTS updateStoreOutstanding ON orders;
CREATE TRIGGER updateStoreOutstanding
AFTER INSERT ON orders
FOR EACH ROW
EXECUTE FUNCTION update_store_outstanding();
SQL

bash "$SCRIPT_DIR/../../configure_postgres.sh" \
  "$database_name" \
  "$TRIGGER_SQL"

echo ">>> Inserting data row-by-row with triggers (11 runs, times recorded by trigger_data_insert.py)..."

python "$SCRIPT_DIR/trigger_data_insert.py" \
  --mode with_trigger \
  --file "$tbl_file_path"

echo ">>> Measuring query performance WITH triggers using base_postgres.sh..."

# Vendor outstanding lookups via maintained aggregate table
vendor_id=10
for i in $(seq 1 11); do
  bash "$SCRIPT_DIR/../../base_postgres.sh" \
    "$database_name" \
    "SELECT amount FROM vendorOutstanding WHERE vendorid = '${vendor_id}';" \
    "with_trigger_vendor" \
    "$(basename "$query_output_csv")"
  vendor_id=$((vendor_id + 1))
done

# Store outstanding lookups via maintained aggregate table
store_id=10
for i in $(seq 1 11); do
  bash "$SCRIPT_DIR/../../base_postgres.sh" \
    "$database_name" \
    "SELECT amount FROM storeOutstanding WHERE storeid = '${store_id}';" \
    "with_trigger_store" \
    "$(basename "$query_output_csv")"
  store_id=$((store_id + 1))
done

echo ">>> Restoring tables to pre-insertion state after WITH-triggers experiment..."

bash "$SCRIPT_DIR/../../configure_postgres.sh" \
  "$database_name" \
  "DELETE FROM orders WHERE ordernum > ${initial_max_ordernum};"

###############################################################################
# 2. WITHOUT TRIGGERS
###############################################################################

echo ">>> Dropping triggers and functions (WITHOUT triggers)..."

read -r -d '' DROP_TRIGGERS_SQL <<'SQL'
DROP TRIGGER IF EXISTS updateVendorOutstanding ON orders;
DROP TRIGGER IF EXISTS updateStoreOutstanding ON orders;
DROP FUNCTION IF EXISTS update_vendor_outstanding();
DROP FUNCTION IF EXISTS update_store_outstanding();
SQL

bash "$SCRIPT_DIR/../../configure_postgres.sh" \
  "$database_name" \
  "$DROP_TRIGGERS_SQL"

echo ">>> Inserting data row-by-row WITHOUT triggers (11 runs, times recorded by trigger_data_insert.py)..."

python "$SCRIPT_DIR/trigger_data_insert.py" \
  --mode without_trigger \
  --file "$tbl_file_path"

echo ">>> Measuring query performance WITHOUT triggers using base_postgres.sh..."

# Vendor aggregate using explicit join and SUM
vendor_id=10
for i in $(seq 1 11); do
  bash "$SCRIPT_DIR/../../base_postgres.sh" \
    "$database_name" \
    "SELECT SUM(orders.quantity * item.price)
     FROM orders
     JOIN item ON orders.itemnum = item.itemnum
     WHERE orders.vendorid = '${vendor_id}';" \
    "without_trigger_vendor" \
    "$(basename "$query_output_csv")"
  vendor_id=$((vendor_id + 1))
done

# Store aggregate using explicit join and SUM
store_id=10
for i in $(seq 1 11); do
  bash "$SCRIPT_DIR/../../base_postgres.sh" \
    "$database_name" \
    "SELECT SUM(orders.quantity * item.price)
     FROM orders
     JOIN item  ON orders.itemnum = item.itemnum
     JOIN store ON orders.storeid = store.storeid
     WHERE store.storeid = '${store_id}';" \
    "without_trigger_store" \
    "$(basename "$query_output_csv")"
  store_id=$((store_id + 1))
done

echo ">>> Restoring orders table to pre-insertion state after WITHOUT-triggers experiment..."

bash "$SCRIPT_DIR/../../configure_postgres.sh" \
  "$database_name" \
  "DELETE FROM orders WHERE ordernum > ${initial_max_ordernum};"

echo "Aggregate Maintenance (triggers vs no triggers) experiment finished."
