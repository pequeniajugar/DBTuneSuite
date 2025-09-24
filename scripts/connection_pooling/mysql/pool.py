# === pool.py (MySQL with socket/TCP, QueuePool) ===
import time
import threading
import random
import argparse
import csv
from pathlib import Path
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool

random.seed(42)

def parse_args():
    ap = argparse.ArgumentParser(description="MySQL pooled insert benchmark")
    ap.add_argument("--db", required=True)
    ap.add_argument("--max-conn", type=int, required=True)
    ap.add_argument("--pool-size", type=int, required=True)
    ap.add_argument("--threads", type=int, required=True)
    # connection params
    ap.add_argument("--user", required=True)
    ap.add_argument("--password", default="")
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--port", type=int, default=3306)
    ap.add_argument("--socket", default="")
    return ap.parse_args()

def make_db_url(args):
    if args.socket:
        return f"mysql+pymysql://{args.user}:{args.password}@/{args.db}?unix_socket={args.socket}"
    else:
        return f"mysql+pymysql://{args.user}:{args.password}@{args.host}:{args.port}/{args.db}"

def generate_insert_query(start_ssnum):
    vals = []
    for i in range(5):
        ssnum = start_ssnum + i + 1
        vals.append(
            f"({ssnum}, 'Employee_{ssnum}', {random.randint(0,9999)}, "
            f"{random.randint(0,9999)}, {random.randint(0,100)}, {random.randint(0,100)})"
        )
    return "INSERT INTO employees (ssnum, name, lat, longitude, hundreds1, hundreds2) VALUES " + ", ".join(vals)

def get_max_ssnum(conn):
    return conn.execute(text("SELECT COALESCE(MAX(ssnum),0) FROM employees")).scalar()

def delete_new_rows(conn, cutoff):
    conn.execute(text("DELETE FROM employees WHERE ssnum > :c"), {"c": cutoff})
    conn.commit()

def set_mysql_max_connections(engine, val):
    with engine.connect() as conn:
        conn.execute(text(f"SET GLOBAL max_connections = {val}"))

def create_pool(db_url, pool_size):
    return create_engine(
        db_url,
        poolclass=QueuePool,
        pool_size=pool_size,
        max_overflow=0,
        pool_timeout=None,
        pool_recycle=1800,
    )

def run_experiment(thread_id, query, exec_times, resp_times, engine):
    attempt = 0
    while attempt < 2000:
        try:
            start_resp = time.perf_counter()
            start_cpu = time.process_time()
            conn = engine.connect()
            conn.execute(text(query))
            conn.commit()
            conn.close()
            exec_times.append(round(time.process_time() - start_cpu, 6))
            resp_times.append(round(time.perf_counter() - start_resp, 6))
            break
        except Exception:
            attempt += 1

def write_result_csv(db_name, mode, pool_or_max, threads, exec_time, resp_time):
    script_dir = Path(__file__).resolve().parent
    out = script_dir / "results"
    out.mkdir(parents=True, exist_ok=True)
    fp = out / f"{db_name}_pooling.csv"
    new = not fp.exists()
    with open(fp, "a", newline="") as f:
        w = csv.writer(f)
        if new:
            w.writerow(["execution_time", "response_time", "mode", "pool_or_max_conn", "threads"])
        w.writerow([f"{exec_time:.6f}", f"{resp_time:.6f}", mode, pool_or_max, threads])

def main():
    args = parse_args()
    db_url = make_db_url(args)

    engine = create_pool(db_url, args.pool_size)
    set_mysql_max_connections(engine, args.max_conn)

    conn = engine.connect()
    base = get_max_ssnum(conn)
    queries = {i + 1: generate_insert_query(base + i * 5) for i in range(args.threads)}

    threads, execs, resps = [], [], []
    t0, c0 = time.perf_counter(), time.process_time()
    for i in range(args.threads):
        t = threading.Thread(target=run_experiment, args=(i + 1, queries[i + 1], execs, resps, engine))
        threads.append(t)
        t.start()
    for t in threads:
        t.join()
    t1, c1 = time.perf_counter(), time.process_time()

    total_cpu = c1 - c0
    total_wall = t1 - t0

    print(f"POOL | Threads={args.threads} | Pool={args.pool_size} | max_conn={args.max_conn}")
    print(f"  CPU Time:  {total_cpu:.4f}s")
    print(f"  Wall Time: {total_wall:.4f}s")
    print(f"  Success:   {len(execs)}/{args.threads}")

    delete_new_rows(conn, base)
    conn.close()
    engine.dispose()

    write_result_csv(args.db, "pool", args.pool_size, args.threads, total_cpu, total_wall)

if __name__ == "__main__":
    main()
