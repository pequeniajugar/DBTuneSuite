import time
import subprocess
from pathlib import Path
import psycopg2

# ==== Config (PostgreSQL) ====
PG_USER = "aa10733"
PG_DATABASE = "TPC_H1"
PG_HOST = "localhost"
PG_PORT = 10000

# Path to your data and batching
DATA_FILE = "/data/aa10733/database1/lineitem.tbl"
TEMP_DIR = "./batches"
BATCH_SIZES = [100, 1000, 10000, 50000, 100000]
RUNS_PER_BATCH = 10

# TPC-H lineitem columns in order (adjust only if your table differs)
LINEITEM_COLUMNS = """(
    l_orderkey, l_partkey, l_suppkey, l_linenumber,
    l_quantity, l_extendedprice, l_discount, l_tax,
    l_returnflag, l_linestatus, l_shipdate, l_commitdate,
    l_receiptdate, l_shipinstruct, l_shipmode, l_comment
)"""

Path(TEMP_DIR).mkdir(parents=True, exist_ok=True)

def pg_conn():
    # Use .pgpass or env vars for password if needed
    return psycopg2.connect(
        dbname=PG_DATABASE,
        user=PG_USER,
        host=PG_HOST,
        port=PG_PORT,
    )

def run_pg_sql(sql):
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)

def count_lines(path, nonempty=False):
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        if nonempty:
            return sum(1 for line in f if line.strip())
        return sum(1 for _ in f)

def copy_batch(cur, batch_path):
    copy_sql = f"""
        COPY lineitem {LINEITEM_COLUMNS}
        FROM '{'/data/aa10733/'+batch_path}' DELIMITER '|';
    """
    try:
        cur.execute(copy_sql)
    except Exception as e:
        print(f"[ERROR] COPY failed on: {batch_path}")
        raise
# ===== Benchmark loop =====
for batch_size in BATCH_SIZES:
    print(f"=== Testing with BATCH_SIZE={batch_size} ===")

    subprocess.run(["rm", "-f"] + list(map(str, Path(TEMP_DIR).glob("batch_*"))))
    subprocess.run(["split", "-l", str(batch_size), DATA_FILE, f"{TEMP_DIR}/batch_"], check=True)

    for run in range(1, RUNS_PER_BATCH + 1):
        print(f"Run #{run} with batch size {batch_size}")

        run_pg_sql("TRUNCATE TABLE lineitem;")
        with pg_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM lineitem;")
                actual_count = cur.fetchone()[0]

        start_time = time.time()
        cpu_start = time.process_time()

        with pg_conn() as conn:
            for batch_file in sorted(Path(TEMP_DIR).glob("batch_*")):
                with conn.cursor() as cur:
                    copy_batch(cur, str(batch_file))
                conn.commit()

        cpu_end = time.process_time()
        end_time = time.time()
        with pg_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM lineitem;")
                actual_count = cur.fetchone()[0]

        real_time = round(end_time - start_time, 6)
        cpu_time = round(cpu_end - cpu_start, 6)

        print(f"{cpu_time} {real_time} {'OK' if actual_count==120515 else 'MISMATCH'}")
