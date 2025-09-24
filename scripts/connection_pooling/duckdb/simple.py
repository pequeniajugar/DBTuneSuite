# === simple.py (DuckDB "NullPool"-style with CSV logging) ===
import time
import threading
import random
import argparse
import csv
from pathlib import Path
import duckdb

random.seed(42)

def parse_args():
    ap = argparse.ArgumentParser(description="DuckDB simple insert benchmark (no pooling / NullPool-style)")
    ap.add_argument("--db", required=True, help="Path to DuckDB database file")
    ap.add_argument("--threads", type=int, required=True, help="Number of worker threads")
    return ap.parse_args()

# --- "NullPool" placeholder for DuckDB: always returns a brand-new connection ---
class NullPoolDuck:
    def __init__(self, db_path: str):
        self.db_path = db_path
    def connect(self):
        # always create a fresh connection (no pooling)
        return duckdb.connect(self.db_path)

def generate_insert_query(start_ssnum):
    values = []
    for i in range(5):
        ssnum = start_ssnum + i + 1
        values.append(
            f"({ssnum}, 'Employee_{ssnum}', {random.randint(0,9999)}, "
            f"{random.randint(0,9999)}, {random.randint(0,100)}, {random.randint(0,100)})"
        )
    return "INSERT INTO employees (ssnum, name, lat, longitude, hundreds1, hundreds2) VALUES " + ", ".join(values)

def get_max_ssnum(conn):
    return conn.execute("SELECT COALESCE(MAX(ssnum),0) FROM employees").fetchone()[0]

def delete_new_rows(conn, cutoff):
    conn.execute("DELETE FROM employees WHERE ssnum > ?", [cutoff])
    conn.commit()

def run_experiment(query, exec_times, resp_times, pool: NullPoolDuck):
    attempt = 0
    while attempt < 2000:
        try:
            start_resp = time.perf_counter()
            start_cpu = time.process_time()

            conn = pool.connect()
            conn.execute(query)
            conn.commit()
            conn.close()

            exec_times.append(round(time.process_time() - start_cpu, 6))
            resp_times.append(round(time.perf_counter() - start_resp, 6))
            break
        except Exception:
            attempt += 1

def write_result_csv(db_path, mode, pool_or_max, threads, exec_time, resp_time):
    script_dir = Path(__file__).resolve().parent
    results_dir = script_dir / "results"
    results_dir.mkdir(parents=True, exist_ok=True)
    db_name = Path(db_path).stem
    out_path = results_dir / f"duckdb_{db_name}_pooling.csv"

    file_exists = out_path.exists()
    with open(out_path, "a", newline="") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["execution_time", "response_time", "mode", "pool_or_max_conn", "threads"])
        writer.writerow([f"{exec_time:.6f}", f"{resp_time:.6f}", mode, pool_or_max, threads])

def main():
    args = parse_args()
    db_path = args.db
    pool = NullPoolDuck(db_path)

    # base max ssnum
    conn = pool.connect()
    base = get_max_ssnum(conn)
    conn.close()

    # prepare queries
    queries = {i+1: generate_insert_query(base + i*5) for i in range(args.threads)}

    # run
    threads, execs, resps = [], [], []
    t0, c0 = time.perf_counter(), time.process_time()
    for i in range(args.threads):
        t = threading.Thread(target=run_experiment, args=(queries[i+1], execs, resps, pool))
        threads.append(t)
        t.start()
    for t in threads:
        t.join()
    t1, c1 = time.perf_counter(), time.process_time()

    total_cpu = c1 - c0
    total_wall = t1 - t0

    print(f"SIMPLE | Threads={args.threads} (DuckDB NullPool-style)")
    print(f"  CPU Time:  {total_cpu:.4f}s")
    print(f"  Wall Time: {total_wall:.4f}s")
    print(f"  Success:   {len(execs)}/{args.threads}")

    # cleanup new rows
    conn = pool.connect()
    delete_new_rows(conn, base)
    conn.close()

    # log to CSV (pool_or_max kept as '-' for compatibility)
    write_result_csv(db_path, "simple", "-", args.threads, total_cpu, total_wall)

if __name__ == "__main__":
    main()
