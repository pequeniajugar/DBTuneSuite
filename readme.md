# DBMS setup



# data generation



# scripts for experiments

## connection pooling

This experiment evaluates the performance of PostgreSQL under different connection strategies using the **SQLAlchemy** library in Python.  

- **`simple.py`**: uses **NullPool** (no pooling, each connection is created and closed independently).  
- **`pool.py`**: uses **QueuePool** (connection pooling provided by SQLAlchemy).  

---

### Experiment Setup

- **Modes**:  
  - `simple` (NullPool)  
  - `pool` (QueuePool)  

- **Pool size / Max connections**:  
  - 25  
  - 50  
  - 100  

- **Number of threads**:  
  - 10  
  - 100  
  - 500  

- **Runs**:  
  - Each configuration is executed **11 times**.  

---

### Execution

The experiment is controlled by the script **`connectionpooling.sh`**, which sequentially runs `simple.py` and `pool.py` for all parameter combinations.  

Example workflow:
1. Set `pool_size = max_connection = 25`, run with 10, 100, 500 threads.  
2. Repeat with `pool_size = max_connection = 50`.  
3. Repeat with `pool_size = max_connection = 100`.  
4. Each case runs 11 iterations for robustness.  

---

### Results

- Results are stored **cumulatively** in: ./results/{db_name}_pooling.csv

- The CSV file contains the following headers:

  ```bash
  "execution_time", "response_time", "mode", "pool_or_max_conn", "threads"

- Each row corresponds to one experimental run.

### Codes

#### postgresql

**Reminder:**

postgresql needs to restart the server to modify the max_connection configuration. We use base.sh to control the restarting process. Remember to change the settings in base.sh.

**How to use:**

```shell
bash connectionpooling.sh <database_name> <user> <password> <host> <port>
# or you can directly modify the settings in connectionpooling.sh so that you don't need to pass the parameters into the execution
```

