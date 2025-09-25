# DBMS setup for nyu students



# data generation



# scripts for experiments

## face lift

## hash vs btree

this experiment includes 3 kinds of queries: multipoint, point and range

### Experiment setup

- **Database**:  employees (do not create the indexes)

- **Modes**: 

  we force the index in these experiments

  - no index
  - hash index
  - b+ tree index

- **Runs**:  

  - Each configuration is executed **11 times**.  

### Execution

The experiments are orchestrated by **run_dbsystem.sh**. Each script contains three query modes and 

- calls **configure_dbsystem.sh** to adjust settings such as storage engine and indexes
- calls **base_dbsystem.sh** to execute the queries.

### Results

- Results are stored **cumulatively** in: ./results/{db_name}_pooling.csv

- The CSV file contains the following headers:

  ```bash
  "dbms","label","iteration","execution_time","response_time"
  ```

- label will be:
  ``` bash
  "no_index","hash_index","btree_index"

- Each row corresponds to one experimental run.

### Codes

#### multipoint

all the codes are in https://github.com/pequeniajugar/dbtunning_experiements/tree/main/scripts/hash_vs_btree_multipoint

#### point

all the codes are in https://github.com/pequeniajugar/dbtunning_experiements/tree/main/scripts/hash_vs_btree_point

#### Range

all the codes are in https://github.com/pequeniajugar/dbtunning_experiements/tree/main/scripts/hash_vs_btree_range

##### mysql

**Reminder:**

- We use the **MEMORY** engine. Every time the service loses connection, the table will be cleared. If switching the engine fails, drop and recreate the tables.
- To enable the MEMORY engine to store larger datasets, we reset the `max_heap_table_size` and `tmp_table_size` parameters to **16G** in our experiments. Please modify these sizes if you are working with larger datasets. After the experiments, we reset them back to the default value of **16MB**. Adjust this if your system’s default is different.
- Whenever we change the index type, we need to drop the old index and recreate the new one.
- please **modify **the **db name** and **file path** in run_mysql.sh before starting.

```bash
#modify the dbname and file path in run_mysql.sh
bash run_mysql.sh
```

##### Mariadb

**Reminder:**

- We use the **MEMORY** engine. Every time the service loses connection, the table will be cleared. If switching the engine fails, drop and recreate the tables.
- To enable the MEMORY engine to store larger datasets, we reset the `max_heap_table_size` and `tmp_table_size` parameters to **16G** in our experiments. Please modify these sizes if you are working with larger datasets. After the experiments, we reset them back to the default value of **16MB**. Adjust this if your system’s default is different.
- Whenever we change the index type, we need to drop the old index and recreate the new one.
- please **modify **the **db name** and **file path** in run_mariadb.sh before starting.

```bash
#modify the dbname and file path in run_mariadb.sh
bash run_mariadb.sh
```

##### postgresql

**Reminder:**

- Whenever we change the index type, we need to drop the old index and recreate the new one.
- please **modify **the **db name** and **file path** in run_mariadb.sh before starting.

```bash
#modify the dbname and file path in run_postgres.sh
bash run_postgres.sh
```

##### duckdb

duckdb does not support hash and b+tree index, skip

## connection pooling

This experiment evaluates the performance of PostgreSQL under different connection strategies using the **SQLAlchemy** library in Python.  

- **`simple.py`**: uses **NullPool** (no pooling, each connection is created and closed independently).  
- **`pool.py`**: uses **QueuePool** (connection pooling provided by SQLAlchemy).  

---

### Experiment Setup

- **Database**:  employees
  
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
  - Each configuration is executed **11 times**.  please use the last 10 times as data

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

all the codes are in https://github.com/pequeniajugar/dbtunning_experiements/tree/main/scripts/connection_pooling

#### mariadb

modify the database system service settings in connectionpooling.sh 

```bash
bash connectionpooling.sh <database_name>
```

#### mysql

modify the database system service settings in connectionpooling.sh 

```bash
bash connectionpooling.sh <database_name>
```

#### duckdb

```bash
bash connectionpooling.sh <database_name>
```

#### postgresql

**Reminder:**

postgresql needs to restart the server to modify the max_connection configuration. We use base.sh to control the restarting process. Remember to change the settings in base.sh.

**How to use:**

modify the database system service settings in connectionpooling.sh 

```shell
bash connectionpooling.sh <database_name>
```

