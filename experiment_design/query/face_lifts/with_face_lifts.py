import time
import mysql.connector
from tqdm import tqdm
import pandas as pd
import os

# === MariaDB Configuration ===
MARIADB_CONFIG = {
    "host": "localhost",
    "port": 15559,
    "user": "tw3090",
    "password": "64113491Ka",
    "database": "employees10_5",
    "charset": "utf8mb4",
    "collation": "utf8mb4_general_ci"
}

MODIFIED_FILE = "/data/tw3090/employee/employees_facelifts_10_5.csv"  # see data_generation/employees/employees_face_lifts.py
COLUMN_COUNT = 6  # Number of columns in the 'employees' table

import mysql.connector

def maintain(file_path, row_count):
    conn = mysql.connector.connect(**MARIADB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS employees;")
    conn.commit()
    cursor.execute("""
        CREATE TABLE employees (
            ssnum INT,
            name VARCHAR(255),
            lat DECIMAL(15,2),
            longitude DECIMAL(15,2),
            hundreds1 INT,
            hundreds2 INT
        );
    """)
    conn.commit()
    cursor.execute("""
        LOAD DATA LOCAL INFILE '/data/aa10733/database1/employeesindex_10_5.csv'
        INTO TABLE employees
        FIELDS TERMINATED BY ','
        LINES TERMINATED BY '\n'
        IGNORE 1 LINES
        (ssnum, name, lat, longitude, hundreds1, hundreds2);
    """)
    conn.commit()

    if row_count>0:
        insert_query = """
            INSERT INTO employees (
                ssnum, name, lat, longitude, hundreds1, hundreds2
            ) VALUES (%s, %s, %s, %s, %s, %s)
        """
        with open(file_path, "r") as f:
            next(f)  # skip header
            for idx, line in enumerate(f):
                if idx >= row_count:
                    break
                fields = line.strip().split(",")
                cursor.execute(insert_query, fields[:COLUMN_COUNT])
                conn.commit()
    cursor.execute("CREATE INDEX c ON employees (hundreds1) WITH FILLFACTOR=100;")
    conn.commit()
    conn.close()
# === Step: Clear the table by deleting rows with ssnum > 100000 ===
def clear_table():
    conn = mysql.connector.connect(**MARIADB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM employees WHERE ssnum > 100000;")#change to 1_000_000 when doing 10^6
    conn.commit()
    conn.close()
    maintain('', 0)

# === Step: Insert rows one-by-one and record timing at checkpoints ===
def insert_and_track(file_path, conn, total_rows):
    cursor = conn.cursor()
    insert_query = """
        INSERT INTO employees (
            ssnum, name, lat, longitude, hundreds1, hundreds2
        ) VALUES (%s, %s, %s, %s, %s, %s)
    """

    checkpoints = [int(total_rows * p) for p in [0.01, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]]
    checkpoint_set = set(checkpoints)
    checkpoint_times = []

    row_count = 0
    start_real = time.time()
    start_cpu = time.process_time()

    last_real = start_real
    last_cpu = start_cpu
    last_row = 0

    with open(file_path, "r") as f:
        next(f)  # Skip header
        for line in tqdm(f, desc="Inserting rows", total=total_rows):
            fields = line.strip().split(",")

            cursor.execute(insert_query, fields[:COLUMN_COUNT])
            conn.commit()

            row_count += 1
            if row_count in checkpoint_set:
                now_real = time.time()
                now_cpu = time.process_time()

                interval_real = now_real - last_real
                interval_cpu = now_cpu - last_cpu
                inserted = row_count - last_row
                throughput = inserted / interval_real if interval_real > 0 else 0

                checkpoint_times.append((row_count, interval_real, interval_cpu, throughput))
                print(f">>> {row_count} rows inserted: Real={now_real - start_real:.3f}s, "
                      f"CPU={now_cpu - start_cpu:.3f}s, Throughput={throughput:.3f} rows/sec")
                maintain(file_path, row_count)
                last_real = time.time()
                last_cpu = time.process_time()
                last_row = row_count

    return checkpoint_times

# === Step: Detect the segment with the most significant throughput drop ===
def detect_slowdown(timings):
    max_drop = 0
    max_drop_segment = None
    throughputs = [tp for _, _, _, tp in timings]

    for i in range(1, len(throughputs)):
        drop = throughputs[i - 1] - throughputs[i]
        if drop > max_drop:
            max_drop = drop
            max_drop_segment = (i - 1, i)

    if max_drop_segment:
        percent_range = (max_drop_segment[0] * 20, (max_drop_segment[1]) * 20)
        return percent_range, max_drop
    else:
        return None, 0

# === Main Loop: Run the insert experiment 10 times ===
def main():
    if not os.path.exists(MODIFIED_FILE):
        raise FileNotFoundError(f"{MODIFIED_FILE} not found. Please check the path.")

    df_preview = pd.read_csv(MODIFIED_FILE, sep="|", header=None)
    total_rows = len(df_preview)
    print(f"File loaded: {MODIFIED_FILE}, total rows = {total_rows}")

    for run in range(1, 11):  # Run 10 times
        print(f"\n========== INSERT Run #{run} ==========")

        # Clear table before each run
        print("Clearing table before run...")
        clear_table()

        # Connect to the database
        conn = mysql.connector.connect(**MARIADB_CONFIG)

        # === Record total execution time for full insert ===
        overall_start_real = time.time()
        overall_start_cpu = time.process_time()

        timings = insert_and_track(MODIFIED_FILE, conn, total_rows)

        overall_end_real = time.time()
        overall_end_cpu = time.process_time()
        conn.close()

        print(f"\nTotal insert finished: Real={overall_end_real - overall_start_real:.3f}s, "
              f"CPU={overall_end_cpu - overall_start_cpu:.3f}s")

        # Print checkpoint details
        print("\nTiming checkpoints:")
        for row, real, cpu, tp in timings:
            print(f"{row} rows → Real={real:.3f}s, CPU={cpu:.3f}s, Throughput={tp:.3f} rows/sec")

        # Detect slowdown segment
        slowdown_range, drop = detect_slowdown(timings)
        if slowdown_range:
            print(f"\nInsert throughput dropped the most between {slowdown_range[0]}% and {slowdown_range[1]}% "
                  f"(Δ throughput = {drop:.3f} rows/sec)")
        else:
            print("\nNo significant slowdown detected.")

        # Clear table after each run
        print("Clearing table after run...")
        clear_table()


if __name__ == "__main__":
    main()
