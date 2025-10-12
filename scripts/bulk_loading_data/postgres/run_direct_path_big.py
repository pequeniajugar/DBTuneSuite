import time
from tqdm import tqdm
import psycopg2

# === PostgreSQL Configuration ===
POSTGRES_CONFIG = {
    "host": "localhost",
    "port": 10000,                  # change if needed
    "user": "aa10733",
    "dbname": "TPC_H2",
}

COLUMN_COUNT = 16  # Number of columns in the lineitem table

# === Establish connection and clear the table ===
def setup_postgres():
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cursor = conn.cursor()
    # No "USE" in Postgres; DB is selected at connect time
    cursor.execute("DELETE FROM lineitem;")
    conn.commit()
    return conn

# === Method 1: COPY FROM (client-side) ===
def load_data_copy(file_path, conn):
    cursor = conn.cursor()
    cursor.execute("DELETE FROM lineitem;")

    start_real_time = time.time()
    start_cpu_time = time.process_time()

    # Client-side COPY: read file here and stream to server
    # TPC-H files are pipe-delimited; use TEXT with DELIMITER '|'
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        cursor.execute(
                """
                COPY lineitem (
                        l_orderkey, l_partkey, l_suppkey, l_linenumber,
                        l_quantity, l_extendedprice, l_discount, l_tax,
                        l_returnflag, l_linestatus, l_shipdate, l_commitdate,
                        l_receiptdate, l_shipinstruct, l_shipmode, l_comment
                )
                FROM '/data/aa10733/database2/lineitem.tbl'
                DELIMITER '|';
                """
        )
    conn.commit()

    end_real_time = time.time()
    end_cpu_time = time.process_time()

    return end_real_time - start_real_time, end_cpu_time - start_cpu_time
# === Method 2: Single-row INSERT ===
def single_row_insert_experiment(file_path, conn):
    cursor = conn.cursor()
    insert_query = """
        INSERT INTO lineitem (
            l_orderkey, l_partkey, l_suppkey, l_linenumber,
            l_quantity, l_extendedprice, l_discount, l_tax,
            l_returnflag, l_linestatus, l_shipdate, l_commitdate,
            l_receiptdate, l_shipinstruct, l_shipmode, l_comment
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    start_real_time = time.time()
    start_cpu_time = time.process_time()

    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        for line in tqdm(f, desc="Inserting rows"):
            fields = line.strip().split("|")
            cursor.execute(insert_query, fields[:COLUMN_COUNT])
            conn.commit()  # mirrors your original behavior

    end_real_time = time.time()
    end_cpu_time = time.process_time()

    return end_real_time - start_real_time, end_cpu_time - start_cpu_time
# === Experiment: Compare the two import methods ===
def run_combined_experiment(tbl_file_path):
    print("\n=== Method 1: COPY FROM (client-side) ===")
    for run in range(1, 0):
        print(f"\n[COPY] Run #{run}")
        conn = setup_postgres()
        response_time, execution_time = load_data_copy(tbl_file_path, conn)

        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM lineitem;")  # confirm loaded
        row_count = cursor.fetchone()[0]
        conn.close()

        print(f"{execution_time:.4f} {response_time:.4f}")
        print(f"Rows Inserted: {row_count} {'OK' if row_count == 11997996 else 'MISMATCH'}")

    print("\n=== Method 2: Row-by-row INSERT ===")
    for run in range(1, 11):
        print(f"\n[INSERT] Run #{run}")
        conn = setup_postgres()
        response_time, execution_time = single_row_insert_experiment(tbl_file_path, conn)

        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM lineitem;")
        row_count = cursor.fetchone()[0]
        conn.close()

        print(f"{execution_time:.4f} {response_time:.4f}")
        print(f" Rows Inserted: {row_count} {'OK' if row_count == 11997996 else 'MISMATCH'}")

# === Run the experiment ===
tbl_file_path = "./database2/lineitem.tbl"  # path to your .tbl file
run_combined_experiment(tbl_file_path)
