import time
import pandas as pd
import duckdb
from tqdm import tqdm

# DuckDB database path
DUCKDB_DB = "path to database"

# Connect to DuckDB and clear the table
def setup_duckdb():
    conn = duckdb.connect(DUCKDB_DB)
    conn.execute("DELETE FROM lineitem;")
    return conn

# Load data using COPY FROM
def load_data_duckdb(file_path, conn):
    conn.execute("DELETE FROM lineitem;")  # clear table

    start_real_time = time.time()
    start_cpu_time = time.process_time()

    query = f"""
    COPY lineitem FROM '{file_path}' (DELIMITER '|', HEADER FALSE);
    """
    conn.execute(query)

    end_real_time = time.time()
    end_cpu_time = time.process_time()

    response_time = end_real_time - start_real_time
    execution_time = end_cpu_time - start_cpu_time

    return response_time, execution_time

# Batch insert using INSERT with a specified batch size
def batch_insert_duckdb(conn, data, batch_size=100):
    query = """INSERT INTO lineitem
               (l_orderkey, l_partkey, l_suppkey, l_linenumber,
                l_quantity, l_extendedprice, l_discount, l_tax,
                l_returnflag, l_linestatus, l_shipdate, l_commitdate,
                l_receiptdate, l_shipinstruct, l_shipmode, l_comment)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""

    # clear table
    conn.execute("DELETE FROM lineitem;")

    # timing
    start_real_time = time.time()
    start_cpu_time = time.process_time()

    for i in tqdm(range(0, len(data), batch_size), desc=f"Batch {batch_size}"):
        batch = data.iloc[i:i+batch_size].values.tolist()
        conn.executemany(query, batch)

    end_real_time = time.time()
    end_cpu_time = time.process_time()

    response_time = end_real_time - start_real_time
    execution_time = end_cpu_time - start_cpu_time

    return response_time, execution_time

# Read .tbl file (assuming data is '|' delimited)
def read_tbl_file(file_path, columns):
    df = pd.read_csv(file_path, delimiter="|", names=columns, index_col=False)
    df.dropna(axis=1, how='all', inplace=True)  # remove empty column (tbl usually ends with '|')
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

    # initialize DuckDB
    duckdb_conn = setup_duckdb()

    # Test COPY FROM (LOAD DATA) 10 times
    load_response_times = []
    load_execution_times = []

    print("\nRunning COPY FROM (LOAD DATA) 10 times")
    for i in range(10):
        response_time, execution_time = load_data_duckdb(tbl_file, duckdb_conn)
        load_response_times.append(response_time)
        load_execution_times.append(execution_time)
        print(f"  Run {i+1}: Response Time = {response_time:.4f}s, Execution Time = {execution_time:.4f}s")

    print("\nResults for COPY FROM (LOAD DATA):")
    print(f"  Response Times: {load_response_times}")
    print(f"  Execution Times: {load_execution_times}")

    # Test batch insert with batch_size = 100
    batch_size = 100
    batch_response_times = []
    batch_execution_times = []

    print("\nRunning batch insert (batch_size=100) 10 times")
    for i in range(10):
        response_time, execution_time = batch_insert_duckdb(duckdb_conn, data, batch_size)
        batch_response_times.append(response_time)
        batch_execution_times.append(execution_time)
        print(f"  Run {i+1}: Response Time = {response_time:.4f}s, Execution Time = {execution_time:.4f}s")

    print("\nResults for batch insert (batch_size=100):")
    print(f"  Response Times: {batch_response_times}")
    print(f"  Execution Times: {batch_execution_times}")

    duckdb_conn.close()

# Run experiment
tbl_file = "path to your lineitem.tbl"  # replace with actual .tbl file path
run_experiment(tbl_file)
