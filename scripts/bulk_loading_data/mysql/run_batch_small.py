mport time
import subprocess
from pathlib import Path
import mysql.connector as mysql

# ==== Config (MySQL) ====
MYSQL_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "pwd",
    "database": "TPC_H1",           # matches your PG_DATABASE name
    "allow_local_infile": True,     # required for LOAD DATA LOCAL INFILE
}

# Path to your data and batching
DATA_FILE = "/data/aa10733/database1/lineitem.tbl"
TEMP_DIR = "./batches"
BATCH_SIZES = [100, 1000, 10000, 50000, 100000]
RUNS_PER_BATCH = 10

# Expected total rows after full load (adjust to your dataset)
EXPECTED_TOTAL_ROWS = 120515

# TPC-H lineitem columns in order
LINEITEM_COLUMNS_UPPER = """(
    L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER,
    L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX,
    L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE, L_COMMITDATE,
    L_RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT
)"""


Path(TEMP_DIR).mkdir(parents=True, exist_ok=True)

def mysql_conn():
    conn = mysql.connect(**MYSQL_CONFIG)
    with conn.cursor() as cur:
        # Enable local_infile for this session (server must allow it)
        try:
            cur.execute("SET GLOBAL local_infile = 1;")
        except:
            pass
    return conn
def run_mysql_sql(sql):
    with mysql_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        conn.commit()

def load_batch(cur, batch_path_abspath):
    # Use LOCAL so MySQL reads from the client (this Python process) path
    load_sql = f"""
        LOAD DATA LOCAL INFILE '{batch_path_abspath}'
        INTO TABLE LINEITEM
        FIELDS TERMINATED BY '|'
        LINES TERMINATED BY '\n'
        {LINEITEM_COLUMNS_UPPER};
    """
    try:
        cur.execute(load_sql)
    except Exception as e:
        print(f"[ERROR] LOAD DATA failed on: {batch_path_abspath}")
        raise

# ===== Benchmark loop =====
for batch_size in BATCH_SIZES:
    print(f"=== Testing with BATCH_SIZE={batch_size} ===")

    # Clean previous batch files and split anew
    subprocess.run(["rm", "-f"] + list(map(str, Path(TEMP_DIR).glob("batch_*"))))
    subprocess.run(["split", "-l", str(batch_size), DATA_FILE, f"{TEMP_DIR}/batch_"], check=True)

    for run in range(1, RUNS_PER_BATCH + 1):
        print(f"Run #{run} with batch size {batch_size}")

        # Truncate target table
        run_mysql_sql("TRUNCATE TABLE LINEITEM;")

        # (Optional) sanity check
        with mysql_conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM LINEITEM;")
            _ = cur.fetchone()[0]

        # Timers
        start_time = time.time()
        cpu_start = time.process_time()

        # Load each batch
        with mysql_conn() as conn:
            for batch_file in sorted(Path(TEMP_DIR).glob("batch_*")):
                batch_abs = str(batch_file.resolve())
                with conn.cursor() as cur:
                    load_batch(cur, batch_abs)
                conn.commit()

        cpu_end = time.process_time()
        end_time = time.time()

        # Count final rows
        with mysql_conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM LINEITEM;")
            actual_count = cur.fetchone()[0]

        real_time = round(end_time - start_time, 6)
        cpu_time = round(cpu_end - cpu_start, 6)

        print(f"{cpu_time} {real_time} {'OK' if actual_count == EXPECTED_TOTAL_ROWS else 'MISMATCH'}")

