import pymysql
import multiprocessing
import time
import csv

# MariaDB connection details
DB_PARAMS = {
    "host": "localhost",
    "user": "your_user",
    "password": "your_password",
    "database": "your_database",
    "port": 3306
}

CSV_FILE = "updates.csv"  # Path to your CSV file

# Read data from the CSV file and modify row[2] by appending 'a'
def load_csv_data(csv_file):
    """
    Read update data from a CSV file and return a list [(new_name, update_value)]
    where update_value is modified by appending 'a'.
    """
    updates = []
    with open(csv_file, mode="r", newline="") as file:
        reader = csv.reader(file)
        next(reader)  # Skip the header row
        for row in reader:
            # Assuming CSV structure: id, new_name, update_field_value
            modified_value = row[2] + "a"  # Append 'a' to update_field_value
            updates.append((row[1], modified_value))  # (new_name, modified_value)
    return updates

# Define the update task (executed by each process)
def update_task(process_id, use_index, updates):
    """
    Each process executes updates where each UPDATE is in its own transaction.
    """
    conn = pymysql.connect(**DB_PARAMS, autocommit=False)
    cursor = conn.cursor()

    updates_count = 0

    for new_name, value in updates:
        try:
            if use_index:
                sql = f"UPDATE employees SET name = '{new_name}' WHERE hundreds1 = '{value}';"
            else:
                sql = f"UPDATE employees SET name = '{new_name}' WHERE longitude = '{value}';"
            
            cursor.execute(sql)
            conn.commit()  # Commit each update as its own transaction
            updates_count += 1
        except pymysql.MySQLError as e:
            print(f"Process {process_id} error: {e}")
            conn.rollback()  # Rollback if an error occurs
    
    cursor.close()
    conn.close()
    print(f"Process {process_id} completed {updates_count} updates.")

# Concurrent experiment
def run_experiment(use_index):
    """
    Run the concurrent experiment:
    - Two processes concurrently update data
    - Each update is a separate transaction
    - use_index controls whether an index is used
    """
    print(f"\nRunning experiment with {'INDEX' if use_index else 'NO INDEX'}...")

    # Load CSV data with modified values
    updates = load_csv_data(CSV_FILE)

    # Split the dataset so each process handles half
    mid_point = len(updates) // 2
    updates_split = [updates[:mid_point], updates[mid_point:]]

    # Start timing
    start_time = time.time()

    # Create two concurrent processes
    processes = []
    for i in range(2):  # Two processes
        p = multiprocessing.Process(target=update_task, args=(i, use_index, updates_split[i]))
        processes.append(p)
        p.start()

    # Wait for all processes to complete
    for p in processes:
        p.join()

    # End timing
    end_time = time.time()
    total_time = end_time - start_time
    print(f"Experiment completed in {total_time:.4f} seconds.")

# Run experiments (without index & with index)
if __name__ == "__main__":
    run_experiment(use_index=False)  # Test without index
    run_experiment(use_index=True)   # Test with index
