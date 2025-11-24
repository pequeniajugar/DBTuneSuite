import time
import psycopg2
from tqdm import tqdm

# === PostgreSQL Configuration ===
PG_CONFIG = {
    "host": "localhost",
    "port": 10001,
    "user": "tw3090",
    "password": "",  # 如果有密码请填写
    "dbname": "tpch10_7"
}

COLUMN_COUNT = 16  # Number of columns in the lineitem table

# === Establish connection and clear the table ===
def setup_postgres():
    conn = psycopg2.connect(**PG_CONFIG)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE lineitem;")  # ensure clean state
    return conn

# === Method 1: COPY FROM (bulk import) ===
def copy_from_file(file_path, conn):
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE lineitem;")

    start_real_time = time.time()
    start_cpu_time = time.process_time()

    with open(file_path, "r") as f:
        cur.copy_from(f, "lineitem", sep="|", null="", columns=[
            "l_orderkey", "l_partkey", "l_suppkey", "l_linenumber",
            "l_quantity", "l_extendedprice", "l_discount", "l_tax",
            "l_returnflag", "l_linestatus", "l_shipdate", "l_commitdate",
            "l_receiptdate", "l_shipinstruct", "l_shipmode", "l_comment"
        ])

    end_real_time = time.time()
    end_cpu_time = time.process_time()

    conn.commit()
    cur.close()

    return end_real_time - start_real_time, end_cpu_time - start_cpu_time

# === Method 2: Single-row INSERT ===
def single_row_insert_experiment(file_path, conn):
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE lineitem;")

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
            if len(fields) >= COLUMN_COUNT:
                cur.execute(insert_query, fields[:COLUMN_COUNT])

    conn.commit()

    end_real_time = time.time()
    end_cpu_time = time.process_time()
    cur.close()

    return end_real_time - start_real_time, end_cpu_time - start_cpu_time

# === Experiment Runner ===
def run_combined_experiment(tbl_file_path):
    print("\n=== Method 1: COPY FROM (Bulk Import) ===")
    for run in range(1, 10):  # you can increase runs to 10 if needed
        print(f"\n[COPY] Run #{run}")
        conn = setup_postgres()
        response_time, execution_time = copy_from_file(tbl_file_path, conn)

        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM lineitem;")
        row_count = cur.fetchone()[0]
        conn.close()

        print(f"  Response Time: {response_time:.4f}s")
        print(f"  Execution Time: {execution_time:.4f}s")
        print(f"  Rows Inserted: {row_count}")

    print("\n=== Method 2: Row-by-row INSERT ===")
    for run in range(1, 2):
        print(f"\n[INSERT] Run #{run}")
        conn = setup_postgres()
        response_time, execution_time = single_row_insert_experiment(tbl_file_path, conn)

        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM lineitem;")
        row_count = cur.fetchone()[0]
        conn.close()

        print(f"  Response Time: {response_time:.4f}s")
        print(f"  Execution Time: {execution_time:.4f}s")
        print(f"  Rows Inserted: {row_count}")

# === Run ===
if __name__ == "__main__":
    tbl_file_path = "/data/tw3090/tpch/tpch10_7/filtered_lineitem.tbl"  # change path as needed
    run_combined_experiment(tbl_file_path)
