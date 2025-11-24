# data generation

refer to data_generation directory

### tpch

https://www.tpc.org/tpch/

use the tools to generate data set and create tables

### others

the codes for datageneration are in *.py files

the codes for creating tables are in *_schema.txt

# scripts for experiments

## data loading

**data and table**

- tpch-lineitem

These experiments evaluate bulk loading strategies for a TPCH-style `lineitem` table.

- **Path-based load** (single `LOAD DATA` / COPY operation)  
- **Batch load** with different batch sizes (split input file, load each batch separately)

Scripts are under `scripts/bulk_loading_data`:

- MySQL: `scripts/bulk_loading_data/mysql/path.py`, `batch.py`  
- PostgreSQL: `scripts/bulk_loading_data/postgres/path.py`, `batch.py`  
- MariaDB: `scripts/bulk_loading_data/mariadb/path.py`, `batch.py`  
- DuckDB: `scripts/bulk_loading_data/duckdb/path.py`, `batch.py`

Each script:

- Truncates the target `lineitem` table.  
- Runs the chosen loading method multiple times.  
- Prints response time and execution time for each run (and saves CSV results where implemented).

**How to run (example for MySQL path-based load):**

```bash
cd scripts/bulk_loading_data/mysql
python path.py
```

**How to run (example for MySQL batch load with different batch sizes):**

```bash
cd scripts/bulk_loading_data/mysql
python batch.py
```

Before running, adjust the connection settings, `DATA_FILE`/file paths, and database names inside the Python scripts.

---

## hash vs btree

this experiment includes 3 kinds of queries: multipoint, point and range

### dataset & table

 employees_index(do not create index)

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



## clustered vs non-clustered vs no index

This experiment compares lookup performance under three index configurations on the `employee_ssnum` table:

- **clustered**: query uses the clustered index on `ssnum`.  
- **non-clustered**: query uses a secondary index on `ssnumpermuted1`.  
- **noindex**: query uses `ssnumpermuted2`, which is not indexed.

Two query shapes are tested:

- **point**: `WHERE ... = {v1}`  
- **range**: `WHERE ... BETWEEN {v1} AND {v2}`

All scripts call `base_*.sh` and log 11 runs per configuration into CSV files under `./results`:

```bash
"dbms","label","iteration","execution_time","response_time"
```

Labels used:

- `"clustered"`
- `"non-clustered"`
- `"noindex"`

### data & table

- employees_ssnum

### Codes

Scripts are under `scripts/clustered_nonclustered_noIndex`:

- **Point queries**: `scripts/clustered_nonclustered_noIndex/point`
  - MySQL: `run_mysql.sh`
  - PostgreSQL: `run_postgres.sh`
  - MariaDB: `run_mariadb.sh`
  - DuckDB: `run_duckdb.sh`
  - Results:
    - `results/mysql_clustered_nonclustered_noindex_point.csv`
    - `results/postgres_clustered_nonclustered_noindex_point.csv`
    - `results/mariadb_clustered_nonclustered_noindex_point.csv`
    - `results/duckdb_clustered_nonclustered_noindex_point.csv`

- **Range queries**: `scripts/clustered_nonclustered_noIndex/range`
  - MySQL: `run_mysql.sh`
  - PostgreSQL: `run_postgres.sh`
  - MariaDB: `run_mariadb.sh`
  - DuckDB: `run_duckdb.sh`
  - Results:
    - `results/mysql_clustered_nonclustered_noindex_range.csv`
    - `results/postgres_clustered_nonclustered_noindex_range.csv`
    - `results/mariadb_clustered_nonclustered_noindex_range.csv`
    - `results/duckdb_clustered_nonclustered_noindex_range.csv`

Each script issues three queries:

- clustered: `SELECT * FROM employee_ssnum WHERE ssnum = {v1};` (point) / `BETWEEN {v1} AND {v2};` (range)  
- non-clustered: `WHERE ssnumpermuted1 = {v1}` / `BETWEEN {v1} AND {v2}`  
- noindex: `WHERE ssnumpermuted2 = {v1}` / `BETWEEN {v1} AND {v2}`

### How to run

From the repository root:

```bash
cd scripts/clustered_nonclustered_noIndex

## point queries
cd point
bash run_mysql.sh
bash run_postgres.sh
bash run_mariadb.sh
bash run_duckdb.sh

## range queries
cd ../range
bash run_mysql.sh
bash run_postgres.sh
bash run_mariadb.sh
bash run_duckdb.sh
```

### Reminder on v1 and v2

All `base_*.sh` helpers use internal variables `v1` and `v2` as placeholders in the query templates:

- `{v1}` and `{v2}` in the SQL are replaced each iteration by numeric values controlled inside `base_*.sh`.  
- For point queries, usually only `{v1}` is used (equality on a single key).  
- For range queries, `{v1}` and `{v2}` define the lower and upper bounds of the key range.

**Before running these experiments**, please adjust the **initial values and step logic of `v1`/`v2`** in the corresponding `base_*.sh` scripts (e.g., `base_mysql.sh`, `base_postgres.sh`) so that:

- `v1`/`v2` fall within the actual key range of your dataset (e.g., valid `ssnum` values).  
- The sequence of values you generate (across the 11 iterations) matches the selectivity and workload you want to study.



## covering index

This experiment compares different index layouts for the same query on the `employee` table, focusing on **covering indexes** vs a **non-covering index**.

Index configurations:

- **ordered covering** (`nc4`): `CREATE INDEX nc4 ON employee(lat, ssnum, name)`  
  - Query: `SELECT ssnum, name FROM employee WHERE lat = {v1};`
- **unordered covering** (`nc5`): `CREATE INDEX nc5 ON employee(ssnum, name, lat)`  
  - Same query as above on `lat`.
- **non-clustered non-covering** (`c`): `CREATE INDEX c ON employee(hundreds1)`  
  - Query: `SELECT * FROM employee WHERE hundreds1 = {v1};`

All scripts call the corresponding `base_*.sh` helper and log 11 timed runs per configuration into CSV files under `./results`:

```bash
"dbms","label","iteration","execution_time","response_time"
```

Labels used:

- `"ordered covering"`
- `"unordered covering"`
- `"non-clustered non-covering"`

### dataset & table

 employees_index

### Codes

Scripts are under `scripts/covering_index`:

- MySQL: `run_mysql.sh`
- PostgreSQL: `run_postgres.sh`
- MariaDB: `run_mariadb.sh`
- DuckDB: `run_duckdb.sh`

Each script:

- Uses `configure_*.sh` (or `duckdb` CLI) to set up `nc4`, `nc5`, and `c` indexes (with `CREATE INDEX IF NOT EXISTS`).  
- Runs the appropriate queries via `base_*.sh` with the labels above.  
- At the end, drops `nc5` as needed and restores `nc4 (lat, ssnum, name)` as the default covering index.

### How to run

From the repository root:

```bash
cd scripts/covering_index

# MySQL
bash run_mysql.sh

# PostgreSQL
bash run_postgres.sh

# MariaDB
bash run_mariadb.sh

# DuckDB
bash run_duckdb.sh
```

**Reminder:**  

- Before running, inspect the **query plans** (e.g., using `EXPLAIN` / `EXPLAIN ANALYZE`) to verify that the DBMS is actually using the covering index as intended.  
- DuckDB 1.1 currently appears **not** to exploit covering indexes in its optimizer, so you may ignore the DuckDB 1.1 results for this experiment.



## fraction scan win (range)

This experiment measures how query performance changes as the **access fraction** increases, using range predicates on one indexed column (`hundreds2`) and one non-indexed column (`longitude`) in the `employee` table.

For each DBMS and dataset (small/big), the scripts:

- Compute `max_value = MAX(hundreds2)` from `employee`.  
- Derive 5 thresholds X from percentages p ∈ {1%, 5%, 10%, 20%, 40%} using:  
  `X = (max_value - 100) * p + 100`.  
- For each threshold X, run:
  - **non-clustered**: `SELECT * FROM employee WHERE hundreds2 < X;`  
  - **noindex**:      `SELECT * FROM employee WHERE longitude < X;`

Labels encode both index type and percentage:

- `"non-clustered_1pct"`, `"non-clustered_5pct"`, ..., `"non-clustered_40pct"`  
- `"noindex_1pct"`, `"noindex_5pct"`, ..., `"noindex_40pct"`

All scripts call `base_*.sh` and log timing results into CSV files under `./results` with the standard schema:

```bash
"dbms","label","iteration","execution_time","response_time"
```

### dataset & table

 employees_index

### Codes

Scripts are under `scripts/fraction_scan_win/range`:

- MySQL: `run_mysql.sh`  
- PostgreSQL: `run_postgres.sh`  
- MariaDB: `run_mariadb.sh`  
- DuckDB: `run_duckdb.sh`

Each script writes to:

- `results/*_big_fraction_scan_win_range.csv`

### How to run

From the repository root:

```bash
cd scripts/fraction_scan_win/range

bash run_mysql.sh
bash run_postgres.sh
bash run_mariadb.sh
bash run_duckdb.sh
```

Before running, adjust the connection settings in each script (DB name, user, password, host, port, DuckDB file path) to match your environment.

## fraction scan win (multipoint)

This experiment evaluates **multipoint queries** at different access fractions on a dedicated table `scanwin_multipoint`, comparing a **non-clustered index** vs a **pure scan**.

Table columns:

- Indexed side: `onepercent1`, `fivepercent1`, `tenpercent1`, `twentypercent1`  
- Scan side:    `onepercent2`, `fivepercent2`, `tenpercent2`, `twentypercent2`

For each access fraction (1%, 5%, 10%, 20%) and each `VALUE` in `[1,5]`, the scripts run:

- **non-clustered**: `SELECT * FROM scanwin_multipoint WHERE onepercent1 = VALUE;` (and similarly for other fractions)  
- **noindex**:      `SELECT * FROM scanwin_multipoint WHERE onepercent2 = VALUE;` (and similarly for other fractions)

Labels include the access fraction:

- `"non-clustered_1pct"`, `"non-clustered_5pct"`, `"non-clustered_10pct"`, `"non-clustered_20pct"`  
- `"noindex_1pct"`, `"noindex_5pct"`, `"noindex_10pct"`, `"noindex_20pct"`

All scripts call `base_*.sh` and write timing results into CSV files under `./results`:

```bash
"dbms","label","iteration","execution_time","response_time"
```

### dataset & table

scanwin_multipoint

### Codes

Scripts are under `scripts/fraction_scan_win/multipoint`:

- MySQL: `run_mysql.sh`  
- PostgreSQL: `run_postgres.sh`  
- MariaDB: `run_mariadb.sh`  
- DuckDB: `run_duckdb.sh`

Each script writes to:

- `results/mysql_fraction_scan_win_multipoint.csv`  
- `results/postgres_fraction_scan_win_multipoint.csv`  
- `results/mariadb_fraction_scan_win_multipoint.csv`  
- `results/duckdb_fraction_scan_win_multipoint.csv`

### How to run

From the repository root:

```bash
cd scripts/fraction_scan_win/multipoint

bash run_mysql.sh
bash run_postgres.sh
bash run_mariadb.sh
bash run_duckdb.sh
```

Before running, ensure:

- The `scanwin_multipoint` table is created and populated using `data_generation/employees/scanwin_multipoint.py`.  
- Connection settings and DuckDB paths in each script match your environment.



## index on small table

This experiment studies the effect of different index configurations on a **small employees table**, for both **UPDATE** and **search** workloads, using multiple concurrent processes.

Index modes:

- `"scan"`: no index used (scan on `longitude`).  
- `"nonclustered"`: nonclustered index on `hundreds2`.  
- `"clustered"`: clustered index on `ssnum` (PRIMARY KEY / clustered index).

Each script runs 10 iterations per mode and records:

- `execution_time` (sum of per-process CPU times)  
- `response_time` (wall-clock time for the whole run)

All timings are stored as CSV files under `./results` with schema:

```bash
"dbms","label","iteration","execution_time","response_time"
```

Labels used:

- `"scan"`, `"nonclustered"`, `"clustered"`

### dataset & table

 employees_smalltable

Schema is same as employees_index

### Codes

Scripts are under `scripts/index_on_small_table`:

- **UPDATE workload (small table)**: `scripts/index_on_small_table/update/mariadb`
  - `small_2process.py` – 2 concurrent processes, 100 updates per process  
  - `big_10process.py` – 10 concurrent processes, 100 updates per process  
  - Results:
    - `results/mariadb_index_small_update_2process.csv`
    - `results/mariadb_index_small_update_10process.csv`

- **SEARCH workload (small table)**: `scripts/index_on_small_table/search/mariadb`
  - `small_2process.py` – 2 concurrent processes, 100 SELECTs per process  
  - `big_10process.py` – 10 concurrent processes, 100 SELECTs per process  
  - Results:
    - `results/mariadb_index_small_search_2process.csv`
    - `results/mariadb_index_small_search_10process.csv`

### How to run

From the repository root:

```bash
# UPDATE workloads
cd scripts/index_on_small_table/update/mariadb

python small_2process.py
python big_10process.py

# SEARCH workloads
cd ../../search/mariadb

python small_2process.py
python big_10process.py
```

Before running, please adjust:

- `DB_PARAMS` (host, port, user, password, database) in each Python script  
- `CSV_FILE` paths for the input data  
- Any assumptions about existing indexes (clustered / nonclustered) to match your schema.

## looping in stored procedures

This experiment compares a single range query with a stored procedure that loops internally and issues many single-row queries on the `lineitem` table.

- **Without loop**:  
  `SELECT * FROM lineitem WHERE l_partkey < {v1};`
- **With loop**:  
  stored procedure `get_lineitems()` that:
  - prepares `SELECT * FROM lineitem WHERE l_partkey = ?;`  
  - loops `i` from 1 to 199 and executes the prepared statement for each `i`.

### dataset & table

tpch-lineitem

### Codes

Scripts are under `scripts/looping_in_stored_procedures`:

- MariaDB: `run_mariadb.sh`

The script:

- Uses `configure_mariadb.sh` to create or replace the `get_lineitems` stored procedure.  
- Calls `base_mariadb.sh` to run both variants with labels:
  - `"without loop"`  
  - `"with loop"`

Results are written into `./results/mariadb_looping_in_stored_procedures.csv` (configurable via `output_csv` in the script).

### How to run

From the repository root:

```bash
cd scripts/looping_in_stored_procedures
bash run_mariadb.sh
```

Before running, adjust the **database name**, **connection settings**, and the **output_csv** path inside `run_mariadb.sh` and `base_mariadb.sh` to match your environment.

### For mysql and postgres

Please use cli to initialize the below method in the respective dbms.

```bash
DELIMITER //
CREATE PROCEDURE get_lineitems()
 BEGIN
 DECLARE i INT DEFAULT 1;
 PREPARE stmt FROM ’SELECT␣*␣FROM␣lineitem␣WHERE␣l_partkey␣=␣?’;
 WHILE i < 200 DO
 SET @param = i;
 EXECUTE stmt USING @param;
 SET i = i + 1;
 END WHILE;
DEALLOCATE PREPARE stmt;
END //
DELIMITER ;
```

## loop with cursor

This experiment measures the overhead of fetching all rows from a table using a **regular SELECT** versus iterating row-by-row using a **cursor inside a stored procedure**.

- **without cursor**: run `SELECT * FROM employees;` directly.  
- **with cursor**: define a stored procedure `fetch_employees()` that opens a cursor on `employees`, loops through all rows, and outputs them one by one, then call `CALL fetch_employees();`.

Both variants are executed via `base_mariadb.sh`, and results are written as a CSV under `./results` with the schema:

```bash
"dbms","label","iteration","execution_time","response_time"
```

Labels used:

- `"without cursor"`
- `"with cursor"`

### dataset & table

employees_index

### Codes

Scripts are under `scripts/loop_with_cursor`:

- MariaDB: `run_mariadb.sh`

The script internally uses:

- `configure_mariadb.sh` to create the `fetch_employees` stored procedure.
- `base_mariadb.sh` to run the two variants and log timings into a single CSV (`output_csv` variable in the script).

### How to run

From the repository root:

```bash
cd scripts/loop_with_cursor

# MariaDB
bash run_mariadb.sh
```

Before running, please adjust the **database name**, **connection settings**, and the **output_csv** path inside `run_mariadb.sh` to match your local environment.

## retrieve needed columns

This experiment compares the cost of fetching **all columns** (`SELECT *`) vs. fetching only the **needed columns** from the `lineitem` table, on both **small** and **big** TPCH datasets, across multiple DBMSs.

### dataset & table

tpch-lineitem

### DBMS

- MySQL: `base_mysql.sh`
- PostgreSQL: `base_postgres.sh`
- DuckDB: `base_duckdb.sh`
- MariaDB: `base_mariadb.sh`

All scripts store timing results into the corresponding `./results/*.txt` or `./results/*_results.csv` files.

### Big dataset (tpch_big)

Scripts are under `scripts/retrieve_needed_columns`:

- MySQL: `run_mysql_big.sh`
- PostgreSQL: `run_postgres_big.sh`
- DuckDB: `run_duckdb_big.sh`
- MariaDB: `run_mariadb_big.sh`

Each script runs:

- `SELECT * FROM lineitem;` (label: `All Columns`)
- `SELECT l_orderkey, l_partkey, l_suppkey, l_shipdate, l_commitdate FROM lineitem;` (label: `Needed Columns`)

**How to use (examples):**

```bash

# MySQL
bash run_mysql_big.sh

# PostgreSQL
bash run_postgres_big.sh

# DuckDB (check and adjust the duckdb_db path in the script if needed)
bash run_duckdb_big.sh

# MariaDB (change the database_name in the script if needed)
bash run_mariadb_big.sh
```

## Unneeded distinct elimination

Scripts are also under `scripts/retrieve_needed_columns`:

- MySQL: `run_mysql.sh`
- PostgreSQL: `run_postgres_small.sh`
- DuckDB: `run_duckdb_small.sh`
- MariaDB: `run_mariadb_small.sh`

These scripts write human-readable timing summaries into `./results/*_small_retrieve_needed_columns.txt`, and internally call the corresponding `base_*.sh` scripts, using labels `All Columns` and `Needed Columns` as appropriate.

**How to use (examples):**

```bash
## eliminate unneeded distinct

This experiment evaluates the performance impact of using an **unnecessary `DISTINCT`** in a join query.  
We compare two logically equivalent queries over the `employee` and `techdept` tables:

- **With Distinct**  
  `SELECT DISTINCT ssnum FROM employee, techdept WHERE employee.dept = techdept.dept;`
- **Without Distinct**  
  `SELECT ssnum FROM employee, techdept WHERE employee.dept = techdept.dept;`

### Experiment setup

- **Tables**: `employee`, `techdept`, `student`  
- **Data distributions**:  
  - uniformly distributed data  
  - fractally distributed data  
- **DBMS**: MySQL, PostgreSQL, MariaDB, DuckDB  
- **Dataset**:  
  - `student_techdept_small` (DuckDB uses the corresponding `.duckdb` database file)

Each script calls the corresponding `base_*.sh` helper and runs the two queries above, with labels:

- `"With Distinct"`
- `"Without Distinct"`

All results are written as CSV files under `./results`, with the schema:

```bash
"dbms","label","iteration","execution_time","response_time"
```

### dataset & table

Employee_stu_dept

### Codes

Scripts are under `scripts/unneeded_distinct_eliminate`:

- MySQL: `run_mysql.sh`  
- PostgreSQL: `run_postgres.sh`  
- MariaDB: `run_mariadb.sh`  
- DuckDB: `run_duckdb.sh`

### How to run

From the repository root:

```bash
cd scripts/unneeded_distinct_eliminate

# MySQL
bash run_mysql.sh

# PostgreSQL
bash run_postgres.sh

# MariaDB
bash run_mariadb.sh

# DuckDB
bash run_duckdb.sh
```

Before running, please adjust the **database names**, **DuckDB database path**, and **connection settings** inside the scripts to match your local environment.

## correlated subquery

This experiment compares three ways to express the same computation: a **correlated subquery**, a join using a **temporary/derived table**, and a query using a **WITH (CTE) clause**.  
All three compute, for each department, the `ssnum` of the employee with the maximum salary.

Labels used:

- `"Correlated Subquery"`
- `"Using Temporary Table"`
- `"Using WITH Clause"`

All scripts write CSV results under `./results` with the standard schema:

```bash
"dbms","label","iteration","execution_time","response_time"
```

### dataset & table

Employee_stu_dept

### Codes

Scripts are under `scripts/correlated_subquery`:

- MySQL (small / big):  
  - `run_mysql_small.sh`  
  - `run_mysql_big.sh`
- PostgreSQL (small / big):  
  - `run_postgres_small.sh`  
  - `run_postgres_big.sh`
- MariaDB (small dataset):  
  - `run_mariadb.sh`
- DuckDB (small dataset):  
  - `run_duckdb.sh`

Each script calls the corresponding `base_*.sh` helper with the three query variants and labels above, writing into a single CSV file (see the `output_csv` variable in each script).

### How to run

From the repository root:

```bash
cd scripts/correlated_subquery

# MySQL
bash run_mysql_small.sh
bash run_mysql_big.sh

# PostgreSQL
bash run_postgres_small.sh
bash run_postgres_big.sh

# MariaDB (small dataset)
bash run_mariadb.sh

# DuckDB (small dataset)
bash run_duckdb.sh
```

Before running, please adjust the **database names**, **DuckDB database path**, and **connection settings** inside the scripts to match your local environment.





## aggregate maintenance (triggers)

This experiment evaluates the cost of maintaining aggregates **with triggers** versus **without triggers** on MariaDB.  
We maintain vendor- and store-level outstanding amounts based on the `orders` and `item` tables.

- **With triggers**: AFTER INSERT triggers on `orders` increment `vendorOutstanding.amount` and `storeOutstanding.amount` on each row insert.  
- **Without triggers**: outstanding amounts are computed on demand using `SUM(orders.quantity * item.price)` over joins.

Insertion time and query time are both measured:

- Insert phase labels: `"with_trigger_insert"`, `"without_trigger_insert"`  
- Query phase labels: `"with_trigger_vendor"`, `"with_trigger_store"`, `"without_trigger_vendor"`, `"without_trigger_store"`

All timings are written as CSV files under `./results`:

- `mariadb_aggregate_triggers_insert.csv` – row-by-row insertion timings  
- `mariadb_aggregate_triggers_queries.csv` – aggregate query timings  

### dataset & table

store

Insertion data generation : triggers_inserts.py

### Codes

Scripts are under `scripts/Aggregate_Maintenance_triggers /mariadb`:

- data for insertion: `data_generation/triggers_inserts.py`
- Python insert driver: `trigger_data_insert.py`
- Experiment orchestrator: `run_mariadb.sh`

The shell script:

- Uses `configure_mariadb.sh` to create/drop the triggers and index on `item(itemnum)`.  
- Calls `trigger_data_insert.py` to run 11 insert experiments (with and without triggers), restoring the tables between runs using the initial max IDs.  
- Uses `base_mariadb.sh` to measure the vendor and store outstanding queries in both modes.

### How to run

From the repository root:

```bash
cd "scripts/Aggregate_Maintenance_triggers /mariadb"

# MariaDB
bash run_mariadb.sh
```

Before running, please adjust:

- `MARIADB_CONFIG` in `trigger_data_insert.py` (host, port, user, password, database)  
- `tbl_file_path` and `database_name` in `run_mariadb.sh`  
- Any connection settings and result CSV paths if your environment differs.



## vertical partitioning

These experiments explore the benefits of vertically partitioning a wide table into multiple narrow tables.

### dataset & table

account

### Scan query

Scripts are under `scripts/vertical_partition_scan`:

- MySQL: `run_mysql_small.sh`, `run_mysql_big.sh`  
- PostgreSQL: `run_postgres_small.sh`, `run_postgres_big.sh`  
- MariaDB: `run_mariadb_small.sh`, `run_mariadb_big.sh`  
- DuckDB: `run_duckdb_small.sh`, `run_duckdb_big.sh`

Each script compares:

- **Without vertical partitioning**: queries on a single wide `account` table.  
- **With vertical partitioning**: equivalent queries over `account1` and `account2`, accessing only needed columns.

Results are written into text files under `./scripts/vertical_partition_scan/results/*vp_scan.txt`.

### Point query

Scripts are under `scripts/vertical_partition_point`:

- MySQL: `run_mysql_small.sh`, `run_mysql_big.sh`  
- PostgreSQL: `run_postgres_small.sh`, `run_postgres_big.sh`  
- MariaDB: `run_mariadb_small.sh`, `run_mariadb_big.sh`  
- DuckDB: `run_duckdb_small.sh`, `run_duckdb_big.sh`

For different access fractions (0%,20%,40%,60%,80%,100%), the scripts execute point queries with and without vertical partitioning by reading from predefined SQL files in `queries/with_vp` and `queries/without_vp`.

**How to run (example for MySQL point queries):**

```bash
cd scripts/vertical_partition_point
bash run_mysql_small.sh
bash run_mysql_big.sh
```

Before running, adjust the database names in the scripts and ensure the vertical-partition tables have been created and populated.

## denormalization

This experiment studies the cost of executing a complex join on a **normalized schema** versus querying a **denormalized table** that pre-joins the same data.

- **Without Denormalization**: query joins `lineitem`, `supplier`, `nation`, `region`.  
- **With Denormalization**: first create `lineitemdenormalized` with the join pre-materialized, then query only that table.

All runs write CSV results under `./results` with columns:

```bash
"dbms","label","iteration","execution_time","response_time"
```

Labels used:

- `"Without Denormalization"`
- `"With Denormalization"`

### dataset & table

tpch

### Codes

Scripts are under `scripts/denormalization`:

- MySQL: `run_mysql.sh`
- PostgreSQL: `run_postgres.sh`
- MariaDB: `run_mariadb.sh`
- DuckDB: `run_duckdb.sh`

All scripts internally call `base_*.sh` and use the big TPCH dataset (e.g., `tpch_big`). Each script also has an `output_csv` variable you can change to redirect the result CSV.

### How to run

From the repository root:

```bash
cd scripts/denormalization

# MySQL
bash run_mysql.sh

# PostgreSQL
bash run_postgres.sh

# MariaDB
bash run_mariadb.sh

# DuckDB
bash run_duckdb.sh
```

Before running, please adjust the **database names**, **DuckDB database path**, and **connection settings** inside the scripts to match your local environment.



## connection pooling

This experiment evaluates the performance of PostgreSQL under different connection strategies using the **SQLAlchemy** library in Python.  

- **`simple.py`**: uses **NullPool** (no pooling, each connection is created and closed independently).  
- **`pool.py`**: uses **QueuePool** (connection pooling provided by SQLAlchemy).  

---

### dataset & table

Employees_index

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
  ```

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

---
