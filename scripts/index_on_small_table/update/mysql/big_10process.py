import pymysql
import multiprocessing
import time
import csv
import random
from pathlib import Path

# MySQL connection details
MYSQL_DB_PARAMS = {
    "host": "localhost",
    "port": 3306,          # change if your MySQL runs on a different port
    "user": "root",        # change to your MySQL user
    "password": "pwd",     # change to your MySQL password
    "database": "employees_small4",  # change DB name if needed
    "charset": "utf8mb4",
}

random.seed(42)

CSV_FILE = "/data/tw3090/employee/employeesindex_small4.csv"  # Path to your CSV file

# Results CSV (change path/name if needed)
ROOT_DIR = Path(__file__).resolve().parents[5]  # repo root
RESULTS_DIR = ROOT_DIR / "results"
RESULTS_DIR.mkdir(parents=True, exist_ok=True)
RESULTS_CSV = RESULTS_DIR / "mysql_index_small_update_10process.csv"


def init_results_csv():
    if not RESULTS_CSV.exists():
        with RESULTS_CSV.open("w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["dbms", "label", "iteration", "execution_time", "response_time"])


def load_csv_data(csv_file, index_type, run_number):
    updates = []
    with open(csv_file, mode="r", newline="") as file:
        reader = csv.reader(file)
        next(reader)  # skip header
        for row in reader:
            if index_type == "clustered":
                condition_value = row[2]  # ssnum
            elif index_type == "nonclustered":
                condition_value = row[4]  # hundreds2
            else:  # scan
                condition_value = row[5]  # longitude
            modified_name = row[1] + str(run_number)
            updates.append((modified_name, condition_value))
    return updates


def update_task(process_id, index_type, updates, result_queue):
    try:
        conn = pymysql.connect(**MYSQL_DB_PARAMS, autocommit=False)
        cursor = conn.cursor()
    except pymysql.MySQLError as e:
        print(f"Process {process_id} - connection failed: {e}")
        return

    updates_count = 0

    if index_type == "clustered":
        # Use PRIMARY index on ssnum (assuming it's the PK)
        sql_template = "UPDATE employees FORCE INDEX (PRIMARY) SET name = %s WHERE ssnum = %s"
    elif index_type == "nonclustered":
        sql_template = "UPDATE employees SET name = %s WHERE hundreds2 = %s"
    else:  # scan
        sql_template = "UPDATE employees SET name = %s WHERE longitude = %s"

    start_cpu = time.process_time()

    for new_name, value in updates:
        try:
            cursor.execute(sql_template, (new_name, value))
            conn.commit()
            updates_count += 1
        except pymysql.MySQLError as e:
            print(f"Process {process_id} error: {e}")
            conn.rollback()

    end_cpu = time.process_time()
    execution_time = end_cpu - start_cpu
    result_queue.put(execution_time)

    cursor.close()
    conn.close()
    print(f"Process {process_id} completed {updates_count} updates.")


def run_experiment(index_type, run_number):
    print(f"\n[Run {run_number}/10] Running experiment with {index_type.upper()} index (MySQL)...")

    updates = load_csv_data(CSV_FILE, index_type=index_type, run_number=run_number)
    # 10 processes * 100 updates each
    sampled_updates = random.sample(updates, 1000)
    updates_split = [sampled_updates[i * 100:(i + 1) * 100] for i in range(10)]

    result_queue = multiprocessing.Queue()
    processes = []

    start_real_time = time.time()

    for i in range(10):
        p = multiprocessing.Process(
            target=update_task,
            args=(i + 1, index_type, updates_split[i], result_queue)
        )
        processes.append(p)
        p.start()

    for p in processes:
        p.join()

    end_real_time = time.time()
    response_time = end_real_time - start_real_time
    execution_time = sum(result_queue.get() for _ in range(10))

    print("Experiment completed.")
    print(f"  Response Time (Real):   {response_time:.4f} seconds")
    print(f"  Execution Time (CPU):   {execution_time:.4f} seconds")

    # Append timing to CSV (label: scan / nonclustered / clustered)
    with RESULTS_CSV.open("a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "mysql",
            index_type,
            run_number,
            f"{execution_time:.6f}",
            f"{response_time:.6f}",
        ])


if __name__ == "__main__":
    init_results_csv()
    for i in range(1, 11):
        run_experiment(index_type="scan", run_number=i)
    for i in range(1, 11):
        run_experiment(index_type="nonclustered", run_number=i)
    for i in range(1, 11):
        run_experiment(index_type="clustered", run_number=i)
