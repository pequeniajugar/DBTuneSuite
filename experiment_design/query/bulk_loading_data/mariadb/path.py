import time
import pandas as pd
import mysql.connector
from tqdm import tqdm

# MariaDB connection configuration
MARIADB_CONFIG = {
    "host": "localhost",
    "port": 15559,
    "user": "tw3090",
    "password": "64113491Ka",  # Replace with your MariaDB password
    "database": "tpch_batch",
    "charset": "utf8mb4",
    "collation": "utf8mb4_general_ci"
}

# Connect to MariaDB and clear the table
def setup_mariadb():
    conn = mysql.connector.connect(**MARIADB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("USE tpch_batch;")
    cursor.execute("DELETE FROM lineitem;")
    conn.commit()
    return conn

# Use LOAD DATA INFILE for bulk loading
def load_data_infile(file_path, conn):
    cursor = conn.cursor()
    cursor.execute("DELETE FROM lineitem;")  # Clear table

    start_real_time = time.time()
    start_cpu_time = time.process_time()

    query = f"""
    LOAD DATA INFILE '{file_path}'
    INTO TABLE lineitem
    FIELDS TERMINATED BY '|' 
    LINES TERMINATED BY '\n';
    """
    cursor.execute(query)
    conn.commit()

    end_real_time = time.time()
    end_cpu_time = time.process_time()

    response_time = end_real_time - start_real_time
    execution_time = end_cpu_time - start_cpu_time

    return response_time, execution_time

# Use batch_size=100 for INSERT
def batch_insert_mariadb(conn, data, batch_size=100):
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
    start_real_time = time.time()
    start_cpu_time = time.process_time()

    for i in tqdm(range(0, len(data), batch_size), desc=f"Batch {batch_size}"):
        batch = data.iloc[i:i+batch_size].values.tolist()
        cursor.executemany(query, batch)
        conn.commit()

    end_real_time = time.time()
    end_cpu_time = time.process_time()

    response_time = end_real_time - start_real_time
    execution_time = end_cpu_time - start_cpu_time

    return response_time, execution_time

# Read .tbl file (assuming data is '|' delimited)
def read_tbl_file(file_path, columns):
    df = pd.read_csv(file_path, delimiter="|", names=columns, index_col=False)
    df.dropna(axis=1, how='all', inplace=True)  # Remove empty columns ('.tbl' files usually end with '|')
    return df

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

    # Test LOAD DATA INFILE 10 times
    load_response_times = []
    load_execution_times = []

    print("\nRunning LOAD DATA INFILE 10 times")
    for i in range(10):
        response_time, execution_time = load_data_infile(tbl_file, mariadb_conn)
        load_response_times.append(response_time)
        load_execution_times.append(execution_time)
        print(f"  Run {i+1}: Response Time = {response_time:.4f}s, Execution Time = {execution_time:.4f}s")

    print("\nResults for LOAD DATA INFILE:")
    print(f"  Response Times: {load_response_times}")
    print(f"  Execution Times: {load_execution_times}")

    # Test batch_size = 100 for 10 times
    batch_size = 100
    batch_response_times = []
    batch_execution_times = []

    print("\nRunning batch insert (batch_size=100) 10 times")
    for i in range(10):
        response_time, execution_time = batch_insert_mariadb(mariadb_conn, data, batch_size)
        batch_response_times.append(response_time)
        batch_execution_times.append(execution_time)
        print(f"  Run {i+1}: Response Time = {response_time:.4f}s, Execution Time = {execution_time:.4f}s")

    print("\nResults for batch insert (batch_size=100):")
    print(f"  Response Times: {batch_response_times}")
    print(f"  Execution Times: {batch_execution_times}")

    mariadb_conn.close()

# Run experiment
tbl_file = "/data/tw3090/tpch/tpch10_5/filtered_lineitem.tbl"  # Replace with actual .tbl file path
run_experiment(tbl_file)
