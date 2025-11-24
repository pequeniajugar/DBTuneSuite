import argparse
import csv
import time
from pathlib import Path

import mysql.connector
from tqdm import tqdm

# === MariaDB Configuration ===
MARIADB_CONFIG = {
    "host": "localhost",
    "port": 15559,
    "user": "tw3090",
    "password": "64113491Ka",
    "database": "store_new_10_5",
    "charset": "utf8mb4",
    "collation": "utf8mb4_general_ci",
}

# Default path to the CSV file with order rows (adjust if needed)
DEFAULT_TBL_FILE_PATH = "/data/tw3090/store/triggers_input.csv"


def get_connection():
    """Create a new MariaDB connection using MARIADB_CONFIG."""
    conn = mysql.connector.connect(**MARIADB_CONFIG)
    cursor = conn.cursor()
    cursor.execute(f"USE {MARIADB_CONFIG['database']};")
    return conn


def clear_orders(conn, initial_max_ordernum: int):
    """Delete orders inserted by previous experimental runs (ordernum > initial_max_ordernum)."""
    cursor = conn.cursor()
    cursor.execute(
        "DELETE FROM orders WHERE ordernum > %s;", (initial_max_ordernum,)
    )
    conn.commit()


def single_row_insert_experiment(file_path: str, conn):
    """
    Insert rows from file_path into orders one by one and measure time.
    Returns (response_time, execution_time, row_count_inserted).
    """
    cursor = conn.cursor()
    insert_query = """
        INSERT INTO orders (
            ordernum, itemnum, quantity, storeid, vendorid
        ) VALUES (%s, %s, %s, %s, %s)
    """

    start_real_time = time.time()
    start_cpu_time = time.process_time()

    inserted_rows = 0
    with open(file_path, "r") as f:
        next(f)  # skip header
        for line in tqdm(f, desc="Inserting rows"):
            fields = line.strip().split(",")
            cursor.execute(insert_query, fields[:COLUMN_COUNT])
            inserted_rows += 1
        conn.commit()

    end_real_time = time.time()
    end_cpu_time = time.process_time()

    return end_real_time - start_real_time, end_cpu_time - start_cpu_time, inserted_rows


def ensure_insert_results_csv(csv_path: Path):
    """Ensure the insert-results CSV exists with the standard header."""
    if not csv_path.exists():
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        with csv_path.open("w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["dbms", "label", "iteration", "execution_time", "response_time"])


def append_insert_result(csv_path: Path, label: str, iteration: int, exec_time: float, resp_time: float):
    with csv_path.open("a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["mariadb", label, iteration, f"{exec_time:.6f}", f"{resp_time:.6f}"])


def run_insert_experiment(tbl_file_path: str, mode: str):
    """
    Run 11 insertion experiments.

    - mode == "with_trigger": clear orders and outstanding tables between runs 1-10,
      leave data after run 11 for subsequent queries.
    - mode == "without_trigger": clear orders between runs 1-10, leave data after run 11.

    Insertion timings are written into results/mariadb_aggregate_triggers_insert.csv
    with labels "with_trigger_insert" or "without_trigger_insert".
    """
    root_dir = Path(__file__).resolve().parents[2]  # go up: mariadb -> Aggregate_Maintenance_triggers -> scripts root
    results_csv = root_dir / "results" / "mariadb_aggregate_triggers_insert.csv"
    ensure_insert_results_csv(results_csv)

    label = "with_trigger_insert" if mode == "with_trigger" else "without_trigger_insert"

    # --- Capture initial max ordernum before any experimental inserts ---
    init_conn = get_connection()
    init_cur = init_conn.cursor()

    init_cur.execute("SELECT COALESCE(MAX(ordernum), 0) FROM orders;")
    initial_max_ordernum = init_cur.fetchone()[0] or 0

    init_conn.close()

    print(f"\n=== Row-by-row INSERT experiment ({mode}) ===")

    for run in range(1, 11 + 1):
        print(f"\n[INSERT - {mode}] Run #{run}")
        conn = get_connection()

        # Ensure clean state before each run: remove rows added after the initial snapshot
        clear_orders(conn, initial_max_ordernum)

        resp_time, exec_time, inserted_rows = single_row_insert_experiment(
            tbl_file_path, conn
        )

        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM orders;")
        row_count = cursor.fetchone()[0]

        print(f"  Response Time: {resp_time:.4f}s")
        print(f"  Execution Time: {exec_time:.4f}s")
        print(f"  Rows Inserted (this run): {inserted_rows}")
        print(f"  Rows in orders after run: {row_count}")

        append_insert_result(results_csv, label, run, exec_time, resp_time)

        conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="Row-by-row INSERT experiment for Aggregate Maintenance (MariaDB)."
    )
    parser.add_argument(
        "--mode",
        choices=["with_trigger", "without_trigger"],
        required=True,
        help="Whether to run the insertion experiment with or without triggers enabled.",
    )
    parser.add_argument(
        "--file",
        default=DEFAULT_TBL_FILE_PATH,
        help="Path to the CSV file containing orders to insert.",
    )
    args = parser.parse_args()

    run_insert_experiment(args.file, args.mode)


if __name__ == "__main__":
    main()
