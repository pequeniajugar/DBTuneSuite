import time
import pandas as pd
import mysql.connector
from tqdm import tqdm

# MariaDB connection configuration
MARIADB_CONFIG = {
    "host": "localhost",
    "port": 15559,
    "user": "tw3090",
    "password": " ",  # Replace with your MariaDB password
    "database": "tpch10_5",
    "charset": "utf8mb4",
    "collation": "utf8mb4_general_ci"
}

# Read .tbl file (assuming data is '|' delimited)
def read_tbl_file(file_path, columns):
    df = pd.read_csv(file_path, delimiter="|", names=columns, index_col=False)
    df.dropna(axis=1, how='all', inplace=True)  # Remove empty columns ('.tbl' files usually end with '|')
    return df

# Connect to MariaDB and clear the table
def setup_mariadb():
    conn = mysql.connector.connect(**MARIADB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("USE tpch10_5;")
    cursor.execute("DELETE FROM lineitem;")
    conn.commit()
    return conn

# Batch insert data (clear table and measure time)
def batch_insert_mariadb(conn, data, batch_size):
    cursor = conn.cursor()
    query = """INSERT INTO lineitem
               (l_orderkey, l_partkey, l_suppkey, l_linenumber,
                l_quantity, l_extendedprice, l_discount, l_tax,
                l_returnflag, l_linestatus, l_shipdate, l_commitdate,
                l_receiptdate, l_shipinstruct, l_shipmode, l_comment)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

    # Clear table
    cursor.execute("DELETE FROM lineitem;")

    # Measure time
    start_real_time = time.time()  # Record real time
    start_cpu_time = time.process_time()  # Record CPU execution time

    for i in tqdm(range(0, 100000, batch_size), desc=f"Batch {batch_size}"):
        batch = data.iloc[i:i+batch_size].values.tolist()
        cursor.executemany(query, batch)
        conn.commit()
    
    end_real_time = time.time()
    end_cpu_time = time.process_time()

    response_time = end_real_time - start_real_time  # Total response time (real time)
    execution_time = end_cpu_time - start_cpu_time  # CPU execution time

    return response_time, execution_time

# Run experiment
def run_experiment(tbl_file):
    columns = [
        "l_orderkey", "l_partkey", "l_suppkey", "l_linenumber",
        "l_quantity", "l_extendedprice", "l_discount", "l_tax",
        "l_returnflag", "l_linestatus", "l_shipdate", "l_commitdate",
        "l_receiptdate", "l_shipinstruct", "l_shipmode", "l_comment"
    ]
    data = read_tbl_file(tbl_file, columns)

    # Initialize MariaDB
    mariadb_conn = setup_mariadb()

    batch_sizes = [1, 20000, 40000, 60000, 80000, 100000]

    for batch_size in batch_sizes:
        print(f"\nRunning batch size: {batch_size}")
        response_times = []
        execution_times = []

        for i in range(10):  # Run experiment 10 times
            response_time, execution_time = batch_insert_mariadb(mariadb_conn, data, batch_size)
            response_times.append(response_time)
            execution_times.append(execution_time)
            print(f"  Run {i+1}: Response Time = {response_time:.4f}s, Execution Time = {execution_time:.4f}s")

        print(f"\nResults for batch size {batch_size}:")
        print(f"  Response Times: {response_times}")
        print(f"  Execution Times: {execution_times}")

    mariadb_conn.close()

# Run experiment
tbl_file = "/data/tw3090/tpch/tpch10_5/filtered_lineitem.tbl"  # Replace with actual .tbl file path
run_experiment(tbl_file)
