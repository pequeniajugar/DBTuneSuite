import time
from tqdm import tqdm
import mysql.connector as mysql
from mysql.connector import errorcode

# === MySQL Configuration ===
MYSQL_CONFIG = {
    "host": "localhost",
    "port": 3306,                 # change if needed
    "user": "root",
    "password": "pwd",               # fill in if needed
    "database": "TPC_H1",
    "allow_local_infile": True,   # required for LOAD DATA LOCAL INFILE
}

COLUMN_COUNT = 16  # Number of columns in the lineitem table
EXPECTED_ROWS = 120515  # adjust if your scale differs

# === Establish connection and clear the table ===
def setup_mysql():
    conn = mysql.connect(**MYSQL_CONFIG)
    cur = conn.cursor()
    # Ensure local_infile is enabled for this session (server must allow it)
    cur.execute("SET GLOBAL local_infile = 1;")

    # Clear table
    cur.execute("DELETE FROM LINEITEM;")
    conn.commit()
    return conn

# === Method 1: LOAD DATA LOCAL INFILE (client-side) ===
def load_data_local_infile(file_path, conn):
    cur = conn.cursor()

    # Start timers
    start_real_time = time.time()
    start_cpu_time = time.process_time()

    load_sql = f"""
    LOAD DATA LOCAL INFILE '{file_path}'
    INTO TABLE LINEITEM
    FIELDS TERMINATED BY '|'
    LINES TERMINATED BY '\n'
    (
        L_ORDERKEY,
        L_PARTKEY,
        L_SUPPKEY,
        L_LINENUMBER,
        L_QUANTITY,
        L_EXTENDEDPRICE,
        L_DISCOUNT,
        L_TAX,
        L_RETURNFLAG,
        L_LINESTATUS,
        L_SHIPDATE,
        L_COMMITDATE,
        L_RECEIPTDATE,
        L_SHIPINSTRUCT,
        L_SHIPMODE,
        L_COMMENT
    );
    """
    cur.execute(load_sql)
    conn.commit()

    end_real_time = time.time()
    end_cpu_time = time.process_time()

    return end_real_time - start_real_time, end_cpu_time - start_cpu_time

# === Method 2: Single-row INSERT ===
def single_row_insert_experiment(file_path, conn):
    cur = conn.cursor()
    insert_query = """
    INSERT INTO LINEITEM (
        L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER,
        L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX,
        L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE, L_COMMITDATE,
        L_RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    start_real_time = time.time()
    start_cpu_time = time.process_time()

    # Commit per row to mirror your original behavior
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        for line in tqdm(f, desc="Inserting rows"):
            # Split on '|' and drop the trailing empty field (due to trailing '|')
            fields = line.rstrip("\n").split("|")
            if fields and fields[-1] == "":
                fields = fields[:-1]

            # Trim to expected columns
            fields = fields[:COLUMN_COUNT]

            # Convert date strings if present (YYYY-MM-DD works directly for MySQL DATE)
            # If your schema uses DATE, no transform needed; if VARCHAR, also fine.
            cur.execute(insert_query, fields)
            conn.commit()

    end_real_time = time.time()
    end_cpu_time = time.process_time()

    return end_real_time - start_real_time, end_cpu_time - start_cpu_time

# === Experiment: Compare the two import methods ===
def run_combined_experiment(tbl_file_path):
    print("\n=== Method 1: LOAD DATA LOCAL INFILE (client-side) ===")
    for run in range(1, 11):
        print(f"\n[LOAD DATA] Run #{run}")
        conn = setup_mysql()
        response_time, execution_time = load_data_local_infile(tbl_file_path, conn)

        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM LINEITEM;")
        row_count = cur.fetchone()[0]
        conn.close()

        print(f"{execution_time:.4f} {response_time:.4f}")
        print(f"  Rows Inserted: {row_count} {'OK' if row_count == EXPECTED_ROWS else 'MISMATCH'}")

    print("\n=== Method 2: Row-by-row INSERT ===")
    for run in range(1, 11):
        print(f"\n[INSERT] Run #{run}")
        conn = setup_mysql()
        response_time, execution_time = single_row_insert_experiment(tbl_file_path, conn)

        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM LINEITEM;")
        row_count = cur.fetchone()[0]
        conn.close()

        print(f"{execution_time:.4f} {response_time:.4f}")
        print(f"  Rows Inserted: {row_count} {'OK' if row_count == EXPECTED_ROWS else 'MISMATCH'}")

# === Run the experiment ===
tbl_file_path = "/data/aa10733/database1/lineitem.tbl"
run_combined_experiment(tbl_file_path)

