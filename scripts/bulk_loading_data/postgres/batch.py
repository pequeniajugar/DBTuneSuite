import time
import subprocess
from pathlib import Path
import pandas as pd
import psycopg2

# === PostgreSQL Config ===
PG_USER = "tw3090"
PG_PASSWORD = ""  
PG_DATABASE = "tpch10_7"
PG_HOST = "localhost"
PG_PORT = 10001

DATA_FILE = "/data/tw3090/tpch/tpch10_7/filtered_lineitem.tbl"  
TEMP_DIR = "./batches"
BATCH_SIZES = [20000, 40000, 60000, 80000, 100000]
RUNS_PER_BATCH = 10  
TABLE_NAME = "lineitem"

# === Ensure TEMP_DIR exists ===
Path(TEMP_DIR).mkdir(parents=True, exist_ok=True)

# === PostgreSQL connection helper ===
def get_pg_conn():
    conn = psycopg2.connect(
        user=PG_USER,
        password=PG_PASSWORD,
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DATABASE,
    )
    conn.autocommit = True
    return conn

def run_pg_cmd(sql):
    conn = get_pg_conn()
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.close()

# === Experiment ===
results = []

for batch_size in BATCH_SIZES:
    print(f"\n=== Testing with BATCH_SIZE={batch_size} ===")

    # Split the original file into small batches
    subprocess.run(["rm", "-f"] + list(map(str, Path(TEMP_DIR).glob("batch_*"))))
    subprocess.run(["split", "-l", str(batch_size), DATA_FILE, f"{TEMP_DIR}/batch_"])

    for run in range(1, RUNS_PER_BATCH + 1):
        print(f"\nRun #{run} with batch size {batch_size}")

        # Truncate the table before each run
        run_pg_cmd(f"TRUNCATE TABLE {TABLE_NAME};")

        start_time = time.time()
        cpu_start = time.process_time()

        conn = get_pg_conn()
        cur = conn.cursor()

        # Loop through each batch file
        for batch_file in sorted(Path(TEMP_DIR).glob("batch_*")):
            with open(batch_file, "r") as f:
                cur.copy_from(
                    f,
                    TABLE_NAME,
                    sep="|",
                    null="",
                    columns=[
                        "l_orderkey", "l_partkey", "l_suppkey", "l_linenumber",
                        "l_quantity", "l_extendedprice", "l_discount", "l_tax",
                        "l_returnflag", "l_linestatus", "l_shipdate", "l_commitdate",
                        "l_receiptdate", "l_shipinstruct", "l_shipmode", "l_comment"
                    ]
                )
        conn.commit()
        cur.close()
        conn.close()

        cpu_end = time.process_time()
        end_time = time.time()

        real_time = round(end_time - start_time, 6)
        cpu_time = round(cpu_end - cpu_start, 6)

        print(f"Real Time: {real_time}s")
        print(f"Execution Time (CPU): {cpu_time}s")

        results.append({
            "batch_size": batch_size,
            "run": run,
            "real_time": real_time,
            "cpu_time": cpu_time
        })

# === Save results ===
df = pd.DataFrame(results)
df.to_csv("postgres_batch_load_results.csv", index=False)
print("\n Results saved to postgres_batch_load_results.csv")
