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
ROOT_DIR = Path(__file__).resolve().parents[5]  # repo root (same structure assumption)
RESULTS_DIR = ROOT_DIR / "results"
RESULTS_DIR.mkdir(parents=True, exist_ok=True)
RESULTS_CSV = RESULTS_DIR / "mysql_index_small_search_10process.csv"


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
            updates.append(condition_value)
    return updates


def query_task(process_id, index_type, values, result_queue):
    try:
        conn = pymysql.connect(**MYSQL_DB_PARAMS, autocommit=False)
        cursor = conn.cursor()
    except pymysql.MySQLError as e:
        print(f"Process {process_id} - connection failed: {e}")
        return

    queries_count = 0

    if index_type == "clustered":
        # clustered -> use PRIMARY index on ssnum (assuming PK)
        sql_template = "SELECT COUNT(*) FROM employees FORCE INDEX (PRIMARY) WHERE ssnum = %s"
    elif index_type == "nonclustered":
        # nonclustered -> assume a secondary index on hundreds2
        sql_template = "SELECT COUNT(*) FROM employees WHERE hundreds2 = %s"
    else:  # scan
        # scan -> no index, condition on longitude
        sql_template = "SELECT COUNT(*) FROM employees WHERE longitude = %s"

    start_cpu = time.process_time()

    for value in values:
        try:
            cursor.execute(sql_template, (value,))
            conn.commit()
            queries_count += 1
        except pymysql.MySQLError as e:
            print(f"Process {process_id} error: {e}")
            conn.rollback()

    end_cpu = time.process_time()
    execution_time = end_cpu - start_cpu
    result_queue.put(execution_time)

    cursor.close()
    conn.close()
    print(f"Process {process_id} completed {queries_count} SELECTs.")


def run_experiment(index_type, run_number, process_count=10, queries_per_process=100):
    print(f"\n[Run {run_number}/10] Running experiment with {index_type.upper()} index (MySQL)...")

    all_values = load_csv_data(CSV_FILE, index_type=index_type, run_number=run_number)
    sampled_values = random.sample(all_values, process_count * queries_per_process)
    split_values = [
        sampled_values[i * queries_per_process:(i + 1) * queries_per_process]
        for i in range(process_count)
    ]

    result_queue = multiprocessing.Queue()
    processes = []

    start_real_time = time.time()

    for i in range(process_count):
        p = multiprocessing.Process(
            target=query_task,
            args=(i + 1, index_type, split_values[i], result_queue)
        )
        processes.append(p)
        p.start()

    for p in processes:
        p.join()

    end_real_time = time.time()
    response_time = end_real_time - start_real_time
    execution_time = sum(result_queue.get() for _ in range(process_count))

    print("Experiment completed.")
    print(f"  Response Time (Real):   {response_time:.4f} seconds")
    print(f"  Execution Time (CPU):   {execution_time:.4f} seconds")

    # Append timing to CSV (label = index_type: scan / nonclustered / clustered)
    with RESULTS_CSV.open("a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "mysql",          # dbms label changed here
            index_type,
            run_number,
            f"{execution_time:.6f}",
            f"{response_time:.6f}",
        ])


if __name__ == "__main__":
    init_results_csv()
    for index_type in ["scan", "nonclustered", "clustered"]:
        for i in range(1, 11):
            run_experiment(index_type=index_type, run_number=i)
