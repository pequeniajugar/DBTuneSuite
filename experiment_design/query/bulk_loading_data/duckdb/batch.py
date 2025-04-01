import time
import pandas as pd
import duckdb
from tqdm import tqdm

# DuckDB database path
DUCKDB_DB = "/data/tw3090/duckdb/tpch_batch"

# Read .tbl file (assuming data is '|' delimited)
def read_tbl_file(file_path, columns):
    df = pd.read_csv(file_path, delimiter="|", names=columns, index_col=False)
    df.dropna(axis=1, how='all', inplace=True)  # Remove empty columns ('.tbl' files usually end with '|')
    return df

# Connect to DuckDB and clear the table
def setup_duckdb():
    conn = duckdb.connect(DUCKDB_DB)
    conn.execute("DELETE FROM lineitem;")  # Clear the table
    return conn

# Batch insert data into DuckDB
def batch_insert_duckdb(conn, data, batch_size):
    query = """INSERT INTO lineitem
               (l_orderkey, l_partkey, l_suppkey, l_linenumber,
                l_quantity, l_extendedprice, l_discount, l_tax,
                l_returnflag, l_linestatus, l_shipdate, l_commitdate,
                l_receiptdate, l_shipinstruct, l_shipmode, l_comment)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""

    conn.execute("DELETE FROM lineitem;")  # Clear the table before inserting

    # Measure time
    start_real_time = time.time()  # Real-world time
    start_cpu_time = time.process_time()  # CPU execution time

    for i in tqdm(range(0, 100000, batch_size), desc=f"Batch {batch_size}"):
        batch = data.iloc[i:i+batch_size].values.tolist()
        conn.executemany(query, batch)  # Batch insert in DuckDB

    end_real_time = time.time()
    end_cpu_time = time.process_time()

    response_time = end_real_time - start_real_time  # Total response time
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

    # Initialize DuckDB
    duckdb_conn = setup_duckdb()

    batch_sizes = [1, 20000, 40000, 60000, 80000, 100000]

    for batch_size in batch_sizes:
        print(f"\nRunning batch size: {batch_size}")
        response_times = []
        execution_times = []

        for i in range(10):  # Run experiment 10 times
            response_time, execution_time = batch_insert_duckdb(duckdb_conn, data, batch_size)
            response_times.append(response_time)
            execution_times.append(execution_time)
            print(f"  Run {i+1}: Response Time = {response_time:.4f}s, Execution Time = {execution_time:.4f}s")

        print(f"\nResults for batch size {batch_size}:")
        print(f"  Response Times: {response_times}")
        print(f"  Execution Times: {execution_times}")

    duckdb_conn.close()

# Run experiment
tbl_file = "/data/tw3090/tpch/tpch10_5/filtered_lineitem.tbl"  # Replace with actual .tbl file path
run_experiment(tbl_file)
