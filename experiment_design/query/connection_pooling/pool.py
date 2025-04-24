# === run_pool_mode.py ===
import time
import threading
import random
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool

# === Experiment Parameters ===
DB_CONFIG = {
    "user": "tw3090",
    "password": "64113491Ka",
    "host": "localhost",
    "port": 15559,
    "database": "employees10_5",
}
MAX_CONN = 25
POOL_SIZE = 25
NUM_THREADS = 500
RETRY_WAIT = 0
DB_URL = f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"

# === Utilities ===
def generate_insert_query(start_ssnum):
    values = []
    for i in range(5):
        ssnum = start_ssnum + i + 1
        values.append(f"({ssnum}, 'Employee_{ssnum}', {random.randint(0, 9999)}, {random.randint(0, 9999)}, {random.randint(0, 100)}, {random.randint(0, 100)})")
    return (
        "INSERT INTO employees (ssnum, name, lat, longitude, hundreds1, hundreds2) VALUES "
        + ", ".join(values)
    )

def get_max_ssnum(conn):
    result = conn.execute(text("SELECT COALESCE(MAX(ssnum), 0) FROM employees"))
    return result.scalar()

def delete_new_rows(conn, max_ssnum):
    conn.execute(text("DELETE FROM employees WHERE ssnum > 100_000"))
    conn.commit()

def count_employees(conn):
    result = conn.execute(text("SELECT COUNT(*) FROM employees"))
    return result.scalar()

def set_mariadb_max_connections(val):
    with create_engine(DB_URL).connect() as conn:
        conn.execute(text(f"SET GLOBAL max_connections = {val}"))

def create_pool():
    return create_engine(
        DB_URL,
        poolclass=QueuePool,
        pool_size=POOL_SIZE,
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
            #time.sleep(RETRY_WAIT)
            attempt += 1

def run():
    set_mariadb_max_connections(MAX_CONN)
    engine = create_pool()
    conn = engine.connect()
    base = get_max_ssnum(conn)
    queries = {i+1: generate_insert_query(base + i * 5) for i in range(NUM_THREADS)}
    delete_new_rows(conn, base - 1)

    threads, execs, resps = [], [], []
    t0, c0 = time.perf_counter(), time.process_time()
    for i in range(NUM_THREADS):
        t = threading.Thread(target=run_experiment, args=(i + 1, queries[i + 1], execs, resps, engine))
        threads.append(t)
        t.start()
    for t in threads:
        t.join()
    t1, c1 = time.perf_counter(), time.process_time()

    print(f"POOL | Threads={NUM_THREADS} | Pool={POOL_SIZE} | max_conn={MAX_CONN}")
    print(f"  CPU Time:  {c1 - c0:.4f}s")
    print(f"  Wall Time: {t1 - t0:.4f}s")
    print(f"  Success:   {len(execs)}/{NUM_THREADS}")
    delete_new_rows(conn, base)
    conn.close()
    engine.dispose()

if __name__ == "__main__":
    run()
