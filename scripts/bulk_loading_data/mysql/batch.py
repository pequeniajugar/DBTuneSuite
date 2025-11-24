import time
import subprocess
from pathlib import Path
import pandas as pd

# --- MySQL Connection Settings (via Unix Socket) ---
MYSQL_USER = "root"
MYSQL_PASSWORD = ""     # Leave blank if no password
MYSQL_SOCKET = "/data/tw3090/mysql/mysql.sock"
MYSQL_BIN = "/home/tw3090/mysql/mysql-commercial-9.1.0-linux-glibc2.28-x86_64/bin/mysql"

MYSQL_DATABASE = "tpch10_7"
DATA_FILE = "/data/tw3090/tpch/tpch10_7/filtered_lineitem.tbl"
TEMP_DIR = "./batches"

BATCH_SIZES = [20000, 40000, 60000, 80000, 100000]
RUNS_PER_BATCH = 10

# --- Ensure TEMP_DIR exists ---
Path(TEMP_DIR).mkdir(parents=True, exist_ok=True)


def run_mysql_cmd(sql, silent=True):
    """Run a MySQL command using the Unix socket connection."""
    cmd = [
        MYSQL_BIN,
        "-u", MYSQL_USER,
        f"--socket={MYSQL_SOCKET}",
        "--local-infile=1",  # Allow client to send local files
        MYSQL_DATABASE,
        "-e", sql
    ]
    if MYSQL_PASSWORD:
        cmd.insert(3, f"-p{MYSQL_PASSWORD}")
    return subprocess.run(cmd, capture_output=silent, text=True)


# === Step 0: Enable local_infile globally ===
print("\n=== Enabling LOCAL INFILE support globally ===")
enable_local = subprocess.run(
    [
        MYSQL_BIN,
        "-u", MYSQL_USER,
        f"--socket={MYSQL_SOCKET}",
        "--local-infile=1",
        "-e", "SET GLOBAL local_infile = 1;"
    ],
    capture_output=True,
    text=True
)

if enable_local.returncode == 0:
    print("local_infile successfully enabled.")
else:
    print("Failed to enable local_infile:")
    print(enable_local.stderr)


# --- Main Experiment ---
results = []

for batch_size in BATCH_SIZES:
    print(f"\n=== Testing with BATCH_SIZE={batch_size} ===")

    # Clean up previous split files
    subprocess.run(["rm", "-f"] + list(map(str, Path(TEMP_DIR).glob("batch_*"))))
    subprocess.run(["split", "-l", str(batch_size), DATA_FILE, f"{TEMP_DIR}/batch_"])

    for run in range(1, RUNS_PER_BATCH + 1):
        print(f"\nRun #{run} with batch size {batch_size}")
        run_mysql_cmd("TRUNCATE TABLE lineitem")

        start_time = time.time()
        cpu_start = time.process_time()

        # --- Load all batch files ---
        for batch_file in sorted(Path(TEMP_DIR).glob("batch_*")):
            sql = f"""
            LOAD DATA LOCAL INFILE '{batch_file}'
            INTO TABLE lineitem
            FIELDS TERMINATED BY '|'
            LINES TERMINATED BY '\\n';
            """
            run_mysql_cmd(sql)

        cpu_end = time.process_time()
        end_time = time.time()

        real_time = round(end_time - start_time, 6)
        cpu_time = round(cpu_end - cpu_start, 6)

        # --- Verify record count (not included in timing) ---
        verify_result = subprocess.run(
            [
                MYSQL_BIN,
                "-u", MYSQL_USER,
                f"--socket={MYSQL_SOCKET}",
                "--local-infile=1",
                MYSQL_DATABASE,
                "-Nse", "SELECT COUNT(*) FROM lineitem;"
            ],
            capture_output=True,
            text=True
        )
        row_count = verify_result.stdout.strip() if verify_result.returncode == 0 else "ERROR"

        print(f"Real Time: {real_time}s")
        print(f"Execution Time (CPU): {cpu_time}s")
        print(f"Rows Inserted: {row_count}")

        results.append({
            "batch_size": batch_size,
            "run": run,
            "real_time": real_time,
            "cpu_time": cpu_time,
            "rows_inserted": row_count
        })


# --- Save results ---
df = pd.DataFrame(results)
df.to_csv("mysql_batch_load_results.csv", index=False)
print("\n Results saved to mysql_batch_load_results.csv")
