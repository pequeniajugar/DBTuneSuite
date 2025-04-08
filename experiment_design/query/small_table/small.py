import pymysql
import multiprocessing
import time
import csv
import random

# MariaDB connection details
DB_PARAMS = {
    "host": "localhost",
    "port": 15559,
    "user": "tw3090",
    "password": "64113491Ka",
    "database": "employees_small",
    "charset": "utf8mb4"
}

# Path to the CSV file containing update data
CSV_FILE = "/data/tw3090/employee/employeesindex_small.csv"

def load_csv_data(csv_file, use_index):
    updates = []
    with open(csv_file, mode="r", newline="") as file:
        reader = csv.reader(file)
        next(reader)  # Skip header row
        for row in reader:
            condition_value = row[3] if use_index else row[4]
            updates.append((row[1], condition_value))  # (new name, condition)
    return updates

def update_task(process_id, use_index, updates, result_queue):
    """
    Function executed in a separate process:
    - Establishes a DB connection
    - Executes updates one by one with COMMIT after each
    - Measures CPU time and reports it via result_queue
    """
    try:
        conn = pymysql.connect(
            host=DB_PARAMS["host"],
            port=DB_PARAMS["port"],
            user=DB_PARAMS["user"],
            password=DB_PARAMS["password"],
            database=DB_PARAMS["database"],
            charset=DB_PARAMS["charset"],
            autocommit=False  # Manual commit for each update
        )
        cursor = conn.cursor()
    except pymysql.MySQLError as e:
        print(f"Process {process_id} - connection failed: {e}")
        return

    updates_count = 0
    sql_template = (
        "UPDATE employees SET name = %s WHERE hundreds1 = %s"
        if use_index else
        "UPDATE employees SET name = %s WHERE longitude = %s"
    )

    start_cpu = time.process_time()

    # Execute updates one by one
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

def run_experiment(use_index, run_number):
    """
    Run one experiment with either index or no index.
    create two child processes that run updates concurrently.
    Measures:
        - Real (wall clock) response time
        - Combined CPU time of the two child processes
    """
    print(f"\n[Run {run_number}/10] Running experiment with {'INDEX' if use_index else 'NO INDEX'}...")

    updates = load_csv_data(CSV_FILE, use_index=use_index)

    # Randomly pick 200 updates and split them for 2 processes
    sampled_updates = random.sample(updates, 200)
    updates_split = [sampled_updates[:100], sampled_updates[100:]]

    result_queue = multiprocessing.Queue()
    processes = []

    start_real_time = time.time()

    # Start 2 parallel update tasks
    for i in range(2):
        p = multiprocessing.Process(
            target=update_task,
            args=(i + 1, use_index, updates_split[i], result_queue)
        )
        processes.append(p)
        p.start()

    # Wait for both processes to finish
    for p in processes:
        p.join()

    end_real_time = time.time()
    response_time = end_real_time - start_real_time
    execution_time = sum(result_queue.get() for _ in range(2))

    print(f"Experiment completed.")
    print(f"  Response Time (Real):   {response_time:.4f} seconds")
    print(f"  Execution Time (CPU):   {execution_time:.4f} seconds")

if __name__ == "__main__":
    # Run 10 experiments without index
    for i in range(1, 11):
        run_experiment(use_index=False, run_number=i)

    # Run 10 experiments with index
    for i in range(1, 11):
        run_experiment(use_index=True, run_number=i)
