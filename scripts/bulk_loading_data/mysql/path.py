import time
import mysql.connector
from tqdm import tqdm

# --- MySQL Connection Settings (via Unix Socket) ---
MYSQL_CONFIG = {
    "unix_socket": "/data/tw3090/mysql/mysql.sock",
    "user": "root",
    "password": "",     # Leave blank if no password
    "database": "tpch10_7",  # Change if needed
    "charset": "utf8mb4",
    "collation": "utf8mb4_general_ci"
}

COLUMN_COUNT = 16  # Number of columns in the lineitem table


# === Establish connection and clear the table ===
def setup_mysql():
    """Connect to MySQL and clear the lineitem table."""
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = conn.cursor()
    cursor.execute("USE tpch_batch;")
    cursor.execute("TRUNCATE TABLE lineitem;")  # TRUNCATE is faster than DELETE
    conn.commit()
    return conn


# === Method 1: LOAD DATA INFILE ===
def load_data_infile(file_path, conn):
    """Import data using LOAD DATA INFILE."""
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE lineitem;")

    start_real_time = time.time()
    start_cpu_time = time.process_time()

    query = f"""
    LOAD DATA LOCAL INFILE '{file_path}'
    INTO TABLE lineitem
    FIELDS TERMINATED BY '|'
    LINES TERMINATED BY '\\n';
    """
    cursor.execute("SET GLOBAL local_infile = 1;")  # Ensure it's enabled
    cursor.execute(query)
    conn.commit()

    end_real_time = time.time()
    end_cpu_time = time.process_time()

    return end_real_time - start_real_time, end_cpu_time - start_cpu_time


# === Method 2: Single-row INSERT ===
def single_row_insert_experiment(file_path, conn):
    """Import data by inserting each row individually."""
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE lineitem;")

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
        conn = setup_mysql()
        response_time, execution_time = load_data_infile(tbl_file_path, conn)

        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM lineitem;")
        row_count = cursor.fetchone()[0]
        conn.close()

        print(f"  Response Time: {response_time:.4f}s")
        print(f"  Execution Time: {execution_time:.4f}s")
        print(f"  Rows Inserted: {row_count}")

    print("\n=== Method 2: Row-by-row INSERT ===")
    for run in range(1, 11):
        print(f"\n[INSERT] Run #{run}")
        conn = setup_mysql()
        response_time, execution_time = single_row_insert_experiment(tbl_file_path, conn)

        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM lineitem;")
        row_count = cursor.fetchone()[0]
        conn.close()

        print(f"  Response Time: {response_time:.4f}s")
        print(f"  Execution Time: {execution_time:.4f}s")
        print(f"  Rows Inserted: {row_count}")


# === Run the experiment ===
tbl_file_path = "/data/tw3090/tpch/tpch10_7/filtered_lineitem.tbl"  # Replace with your actual file
run_combined_experiment(tbl_file_path)
