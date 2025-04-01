import time
import mysql.connector
from tqdm import tqdm

# === MariaDB Configuration ===
MARIADB_CONFIG = {
    "host": "localhost",
    "port": 15559,
    "user": "tw3090",
    "password": "64113491Ka",
    "database": "tpch_batch",
    "charset": "utf8mb4",
    "collation": "utf8mb4_general_ci"
}

COLUMN_COUNT = 16  # Number of columns in the lineitem table

# === Establish connection and clear the table ===
def setup_mariadb():
    conn = mysql.connector.connect(**MARIADB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("USE tpch_batch;")
    cursor.execute("DELETE FROM lineitem;")
    conn.commit()
    return conn

# === Method 1: LOAD DATA INFILE ===
def load_data_infile(file_path, conn):
    cursor = conn.cursor()
    cursor.execute("DELETE FROM lineitem;")

    start_real_time = time.time()
    start_cpu_time = time.process_time()

    query = f"""
    LOAD DATA INFILE '{file_path}'
    INTO TABLE lineitem
    FIELDS TERMINATED BY '|'
    LINES TERMINATED BY '\n';
    """
    cursor.execute(query)
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

    with open(file_path, "r") as f:
        for line in tqdm(f, desc="Inserting rows"):
            fields = line.strip().split("|")
            cursor.execute(insert_query, fields[:COLUMN_COUNT])
            conn.commit()

    end_real_time = time.time()
    end_cpu_time = time.process_time()

    return end_real_time - start_real_time, end_cpu_time - start_cpu_time

# === Experiment: Compare the two import methods ===
def run_combined_experiment(tbl_file_path):
    print("\n=== Method 1: LOAD DATA INFILE ===")
    for run in range(1, 11):
        print(f"\n[LOAD] Run #{run}")
        conn = setup_mariadb()
        response_time, execution_time = load_data_infile(tbl_file_path, conn)

        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM lineitem;") # to confirm if all the dataset has been loaded
        row_count = cursor.fetchone()[0]
        conn.close()

        print(f"  Response Time: {response_time:.4f}s")
        print(f"  Execution Time: {execution_time:.4f}s")
        print(f"  Rows Inserted: {row_count} {'OK' if row_count == 120515 else 'MISMATCH'}")

    print("\n=== Method 2: Row-by-row INSERT ===")
    for run in range(1, 11):
        print(f"\n[INSERT] Run #{run}")
        conn = setup_mariadb()
        response_time, execution_time = single_row_insert_experiment(tbl_file_path, conn)

        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM lineitem;")
        row_count = cursor.fetchone()[0]
        conn.close()

        print(f"  Response Time: {response_time:.4f}s")
        print(f"  Execution Time: {execution_time:.4f}s")
        print(f"  Rows Inserted: {row_count} {'OK' if row_count == 120515 else 'MISMATCH'}")

# === Run the experiment ===
tbl_file_path = "/data/tw3090/tpch/tpch10_5/filtered_lineitem.tbl"  # Replace with your actual .tbl file path
run_combined_experiment(tbl_file_path)
