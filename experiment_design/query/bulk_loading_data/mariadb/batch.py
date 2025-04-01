import time
import subprocess
import os
from pathlib import Path
import pandas as pd

# Config
MYSQL_USER = "tw3090"
MYSQL_PASSWORD = "64113491Ka"
MYSQL_DATABASE = "tpch_batch"
MYSQL_HOST = "localhost"
MYSQL_PORT = 15559

DATA_FILE = "/data/tw3090/tpch/tpch10_5/filtered_lineitem.tbl"
TEMP_DIR = "./batches"
BATCH_SIZES = [100,1000,10000,50000,100000]
RUNS_PER_BATCH = 10

# Ensure TEMP_DIR exists
Path(TEMP_DIR).mkdir(parents=True, exist_ok=True)

# Function to run a MySQL command
def run_mysql_cmd(sql, silent=True):
    cmd = [
        "/usr/local/stow/mariadb-11.4/bin/mysql",
        "-u", MYSQL_USER,
        f"-p{MYSQL_PASSWORD}",
        "-h", MYSQL_HOST,
        "-P", str(MYSQL_PORT),
        MYSQL_DATABASE,
        "-e", sql
    ]
    return subprocess.run(cmd, capture_output=silent, text=True)

# Record results
results = []

# Loop through each batch size
for batch_size in BATCH_SIZES:
    print(f"=== Testing with BATCH_SIZE={batch_size} ===")

    # Split the original file
    subprocess.run(["rm", "-f"] + list(Path(TEMP_DIR).glob("batch_*")))
    subprocess.run(["split", "-l", str(batch_size), DATA_FILE, f"{TEMP_DIR}/batch_"])

    for run in range(1, RUNS_PER_BATCH + 1):
        print(f"Run #{run} with batch size {batch_size}")

        # Truncate table
        run_mysql_cmd("TRUNCATE TABLE lineitem")

        start_time = time.time()
        cpu_start = time.process_time()

        # Load each batch
        for batch_file in sorted(Path(TEMP_DIR).glob("batch_*")):
            sql = f"""
            LOAD DATA LOCAL INFILE '{batch_file}' INTO TABLE lineitem
            FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';
            """
            run_mysql_cmd(sql)

        cpu_end = time.process_time()
        end_time = time.time()

        real_time = round(end_time - start_time, 6)
        cpu_time = round(cpu_end - cpu_start, 6)


        print(f"Real Time: {real_time}s")
        print(f"Execution Time (CPU): {cpu_time}s")