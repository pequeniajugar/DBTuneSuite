import time
import duckdb
from pathlib import Path
import subprocess

# Config
DUCKDB_DB = "path to database"
DATA_FILE = "path to your lineitem.tbl"
TEMP_DIR = "./batches"
BATCH_SIZES = [1, 100, 1000, 10000, 50000, 100000]
RUNS_PER_BATCH = 10

# Ensure temp dir exists
Path(TEMP_DIR).mkdir(parents=True, exist_ok=True)

# Function to run DuckDB command
def run_duckdb_sql(con, sql):
    con.execute(sql)

# Start experiment
for batch_size in BATCH_SIZES:
    print(f"=== Testing with BATCH_SIZE={batch_size} ===")

    # Split original file
    subprocess.run(["rm", "-f"] + list(Path(TEMP_DIR).glob("batch_*")))
    subprocess.run(["split", "-l", str(batch_size), DATA_FILE, f"{TEMP_DIR}/batch_"])

    for run in range(1, RUNS_PER_BATCH + 1):
        print(f"Run #{run} with batch size {batch_size}")

        # Connect & clear table
        con = duckdb.connect(DUCKDB_DB)
        run_duckdb_sql(con, "DELETE FROM lineitem;")

        start_time = time.time()
        cpu_start = time.process_time()

        # Load each batch file
        for batch_file in sorted(Path(TEMP_DIR).glob("batch_*")):
            sql = f"""
            COPY lineitem FROM '{batch_file}' (DELIMITER '|');
            """
            run_duckdb_sql(con, sql)

        cpu_end = time.process_time()
        end_time = time.time()

        real_time = round(end_time - start_time, 6)
        cpu_time = round(cpu_end - cpu_start, 6)

        # Optional: validate inserted rows
        row_count = con.execute("SELECT COUNT(*) FROM lineitem;").fetchone()[0]
        con.close()

        print(f"Real Time: {real_time}s")
        print(f"Execution Time (CPU): {cpu_time}s")
        print(f"Inserted Rows: {row_count}")
