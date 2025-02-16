##  denormalization

schema

https://www.tpc.org/TPC_Documents_Current_Versions/pdf/TPC-H_v3.0.1.pdf

**data type**

only uniformly distributed data

**SF**

0.02 & 2

**with denormalization**

```sql
select L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER,  L_QUANTITY, L_EXTENDEDPRICE,  L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT, R_REGION
from lineitemdenormalized
where R_REGION = 'EUROPE';
```

**without denormalization**

```sql
select L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT, R_NAME
from lineitem, region, supplier, nation
where
L_SUPPKEY = S_SUPPKEY
and S_NATIONKEY = N_NATIONKEY
and N_REGIONKEY = R_REGIONKEY
and R_NAME = 'EUROPE';
```



**mariadb** table creation

```sql
CREATE TABLE region (
    R_REGIONKEY INT,              -- Identifier for region (e.g., 0-4)
    R_NAME CHAR(25),              -- Fixed text, size 25
    R_COMMENT VARCHAR(152)        -- Variable text, size 152
);
LOAD DATA LOCAL INFILE '/data/tw3090/tpch/tpch10_5/region.tbl' 
INTO TABLE region
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n';
```

```sql
CREATE TABLE nation (
    N_NATIONKEY INT,              -- Identifier for nation (e.g., 0-24)
    N_NAME CHAR(25),              -- Fixed text, size 25
    N_REGIONKEY INT,              -- Identifier, Foreign Key to R_REGIONKEY
    N_COMMENT VARCHAR(152)        -- Variable text, size 152
);

LOAD DATA LOCAL INFILE '/data/tw3090/tpch/tpch10_5/nation.tbl' 
INTO TABLE nation
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n';

ALTER TABLE nation ADD PRIMARY KEY (N_NATIONKEY);

ALTER TABLE nation
ADD CONSTRAINT fk_nation_region FOREIGN KEY (N_REGIONKEY) REFERENCES region(R_REGIONKEY);
```

```sql
CREATE TABLE supplier (
    S_SUPPKEY INT,              -- Identifier, SF*10,000 suppliers are populated
    S_NAME CHAR(25),            -- Fixed text, size 25
    S_ADDRESS VARCHAR(40),      -- Variable text, size 40
    S_NATIONKEY INT,            -- Identifier, Foreign Key to N_NATIONKEY
    S_PHONE CHAR(15),           -- Fixed text, size 15
    S_ACCTBAL DECIMAL(15, 2),   -- Decimal value with precision and scale
    S_COMMENT VARCHAR(101)      -- Variable text, size 101
);

LOAD DATA LOCAL INFILE '/data/tw3090/tpch/tpch10_7/supplier.tbl' 
INTO TABLE supplier
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n';

ALTER TABLE supplier ADD PRIMARY KEY (S_SUPPKEY);

ALTER TABLE supplier
ADD CONSTRAINT fk_supplier_nation FOREIGN KEY (S_NATIONKEY) REFERENCES nation(N_NATIONKEY);

```

```sql
CREATE TABLE partsupp (
    PS_PARTKEY INT,
    PS_SUPPKEY INT,
    PS_AVAILQTY INT,
    PS_SUPPLYCOST DECIMAL(15, 2),
    PS_COMMENT VARCHAR(44)
);

LOAD DATA LOCAL INFILE '/data/tw3090/tpch/tpch10_7/partsupp.tbl' 
INTO TABLE partsupp
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n';

ALTER TABLE partsupp ADD PRIMARY KEY (PS_PARTKEY, PS_SUPPKEY);
ALTER TABLE partsupp ADD CONSTRAINT fk_partsupp_supplier FOREIGN KEY (PS_SUPPKEY) REFERENCES supplier(S_SUPPKEY);
```

```sql
CREATE TABLE lineitem (
    L_ORDERKEY INT,               -- Identifier, Foreign Key to O_ORDERKEY
    L_PARTKEY INT,
    L_SUPPKEY INT,                -- Identifier, Foreign Key to S_SUPPKEY
    L_LINENUMBER INT,                -- Integer
    L_QUANTITY DECIMAL(12, 2),       -- Decimal value for quantity
    L_EXTENDEDPRICE DECIMAL(15, 2),  -- Decimal value for extended price
    L_DISCOUNT DECIMAL(4, 2),        -- Decimal value for discount
    L_TAX DECIMAL(4, 2),             -- Decimal value for tax
    L_RETURNFLAG CHAR(1),            -- Fixed text, size 1
    L_LINESTATUS CHAR(1),            -- Fixed text, size 1
    L_SHIPDATE DATE,                 -- Date for shipping
    L_COMMITDATE DATE,               -- Date for commitment
    L_RECEIPTDATE DATE,              -- Date for receipt
    L_SHIPINSTRUCT CHAR(25),         -- Fixed text, size 25
    L_SHIPMODE CHAR(10),             -- Fixed text, size 10
    L_COMMENT VARCHAR(44)            -- Variable text, size 44
);

LOAD DATA LOCAL INFILE '/data/tw3090/tpch/tpch10_7/lineitem.tbl' 
INTO TABLE lineitem
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n';

ALTER TABLE lineitem ADD PRIMARY KEY (L_ORDERKEY, L_LINENUMBER);
ALTER TABLE lineitem ADD CONSTRAINT fk_lineitem_partsupp1 FOREIGN KEY L_PARTKEY REFERENCES partsupp(PS_PARTKEY);
select count(*) from region;
ALTER TABLE lineitem ADD CONSTRAINT fk_lineitem_partsupp2 FOREIGN KEY L_SUPPKEY REFERENCES partsupp(PS_SUPPKEY);

ALTER TABLE lineitem ADD CONSTRAINT fk_lineitem_partsupp FOREIGN KEY (L_PARTKEY, L_SUPPKEY) REFERENCES partsupp(PS_PARTKEY, PS_SUPPKEY);
```

```sql
-- 1. empty table
CREATE TABLE lineitemdenormalized (
    L_ORDERKEY INT,
    L_LINENUMBER INT,
    L_PARTKEY INT,
    L_SUPPKEY INT,
    L_QUANTITY DECIMAL(12, 2),
    L_EXTENDEDPRICE DECIMAL(15, 2),
    L_DISCOUNT DECIMAL(4, 2),
    L_TAX DECIMAL(4, 2),
    L_RETURNFLAG CHAR(1),
    L_LINESTATUS CHAR(1),
    L_SHIPDATE DATE,
    L_COMMITDATE DATE,
    L_RECEIPTDATE DATE,
    L_SHIPINSTRUCT CHAR(25),
    L_SHIPMODE CHAR(10),
    L_COMMENT VARCHAR(44),
    R_REGION TEXT
);

-- 2. insert by using select
INSERT INTO lineitemdenormalized 
SELECT 
    L.L_ORDERKEY,
    L.L_LINENUMBER,
    L.L_PARTKEY,
    L.L_SUPPKEY,
    L.L_QUANTITY,
    L.L_EXTENDEDPRICE,
    L.L_DISCOUNT,
    L.L_TAX,
    L.L_RETURNFLAG,
    L.L_LINESTATUS,
    L.L_SHIPDATE,
    L.L_COMMITDATE,
    L.L_RECEIPTDATE,
    L.L_SHIPINSTRUCT,
    L.L_SHIPMODE,
    L.L_COMMENT,
    R.R_NAME AS R_REGION
FROM 
    lineitem L
JOIN 
    supplier S ON L.L_SUPPKEY = S.S_SUPPKEY
JOIN 
    nation N ON S.S_NATIONKEY = N.N_NATIONKEY
JOIN 
    region R ON N.N_REGIONKEY = R.R_REGIONKEY;


ALTER TABLE lineitemdenormalized ADD PRIMARY KEY (L_ORDERKEY, L_linenumber);
```

**duckdb** table creation

```sql
CREATE TABLE region (
    R_REGIONKEY INT primary key,              -- Identifier for region (e.g., 0-4)
    R_NAME CHAR(25),              -- Fixed text, size 25
    R_COMMENT VARCHAR(152)        -- Variable text, size 152
);

COPY region FROM '/data/tw3090/tpch/tpch10_4/region.tbl' (DELIMITER '|', HEADER FALSE);
```

```sql
CREATE TABLE nation (
    N_NATIONKEY INT primary key,              -- Identifier for nation (e.g., 0-24)
    N_NAME CHAR(25),              -- Fixed text, size 25
    N_REGIONKEY INT references region(R_REGIONKEY),              -- Identifier, Foreign Key to R_REGIONKEY
    N_COMMENT VARCHAR(152)        -- Variable text, size 152
);

COPY nation FROM '/data/tw3090/tpch/tpch10_8/nation.tbl' (DELIMITER '|', HEADER FALSE);
```

```sql
CREATE TABLE supplier (
    S_SUPPKEY INT primary key,              -- Identifier, SF*10,000 suppliers are populated
    S_NAME CHAR(25),            -- Fixed text, size 25
    S_ADDRESS VARCHAR(40),      -- Variable text, size 40
    S_NATIONKEY INT references nation(N_NATIONKEY),            -- Identifier, Foreign Key to N_NATIONKEY
    S_PHONE CHAR(15),           -- Fixed text, size 15
    S_ACCTBAL DECIMAL(15, 2),   -- Decimal value with precision and scale
    S_COMMENT VARCHAR(101)      -- Variable text, size 101
);

COPY supplier FROM '/data/tw3090/tpch/tpch10_7/supplier.tbl' (DELIMITER '|', HEADER FALSE);
```

```sql
CREATE TABLE partsupp (
    PS_PARTKEY INT,
    PS_SUPPKEY INT references supplier(S_SUPPKEY),
    PS_AVAILQTY INT,
    PS_SUPPLYCOST DECIMAL(15, 2),
    PS_COMMENT VARCHAR(44),
    PRIMARY KEY (PS_PARTKEY, PS_SUPPKEY)
);

COPY partsupp FROM '/data/tw3090/tpch/tpch10_7/partsupp.tbl' (DELIMITER '|', HEADER FALSE);
```

```sql
CREATE TABLE lineitem (
    L_ORDERKEY INT,               -- Identifier, Foreign Key to O_ORDERKEY
    L_PARTKEY INT,
    L_SUPPKEY INT,                -- Identifier, Foreign Key to S_SUPPKEY
    L_LINENUMBER INT,                -- Integer
    L_QUANTITY DECIMAL(12, 2),       -- Decimal value for quantity
    L_EXTENDEDPRICE DECIMAL(15, 2),  -- Decimal value for extended price
    L_DISCOUNT DECIMAL(4, 2),        -- Decimal value for discount
    L_TAX DECIMAL(4, 2),             -- Decimal value for tax
    L_RETURNFLAG CHAR(1),            -- Fixed text, size 1
    L_LINESTATUS CHAR(1),            -- Fixed text, size 1
    L_SHIPDATE DATE,                 -- Date for shipping
    L_COMMITDATE DATE,               -- Date for commitment
    L_RECEIPTDATE DATE,              -- Date for receipt
    L_SHIPINSTRUCT CHAR(25),         -- Fixed text, size 25
    L_SHIPMODE CHAR(10),             -- Fixed text, size 10
    L_COMMENT VARCHAR(44),            -- Variable text, size 44
primary key (L_ORDERKEY, L_LINENUMBER),
FOREIGN KEY (L_PARTKEY, L_SUPPKEY) REFERENCES partsupp(PS_PARTKEY, PS_SUPPKEY)
);

COPY lineitem FROM '/data/tw3090/tpch/tpch10_7/lineitem.tbl' (DELIMITER '|', HEADER FALSE);
```

```sql
CREATE TABLE lineitemdenormalized (
    L_ORDERKEY INT,
    L_LINENUMBER INT,
    L_PARTKEY INT,
    L_SUPPKEY INT,
    L_QUANTITY DECIMAL(12, 2),
    L_EXTENDEDPRICE DECIMAL(15, 2),
    L_DISCOUNT DECIMAL(4, 2),
    L_TAX DECIMAL(4, 2),
    L_RETURNFLAG CHAR(1),
    L_LINESTATUS CHAR(1),
    L_SHIPDATE DATE,
    L_COMMITDATE DATE,
    L_RECEIPTDATE DATE,
    L_SHIPINSTRUCT CHAR(25),
    L_SHIPMODE CHAR(10),
    L_COMMENT VARCHAR(44),
    R_REGION TEXT,
    PRIMARY KEY (L_ORDERKEY, L_LINENUMBER)
);

-- insert data
INSERT INTO lineitemdenormalized 
SELECT 
    L.L_ORDERKEY,
    L.L_LINENUMBER,
    L.L_PARTKEY,
    L.L_SUPPKEY,
    L.L_QUANTITY,
    L.L_EXTENDEDPRICE,
    L.L_DISCOUNT,
    L.L_TAX,
    L.L_RETURNFLAG,
    L.L_LINESTATUS,
    L.L_SHIPDATE,
    L.L_COMMITDATE,
    L.L_RECEIPTDATE,
    L.L_SHIPINSTRUCT,
    L.L_SHIPMODE,
    L.L_COMMENT,
    R.R_NAME AS R_REGION
FROM 
    lineitem L
JOIN 
    supplier S ON L.L_SUPPKEY = S.S_SUPPKEY
JOIN 
    nation N ON S.S_NATIONKEY = N.N_NATIONKEY
JOIN 
    region R ON N.N_REGIONKEY = R.R_REGIONKEY;
```

##  vertical partitioning

###  scan

**data type**

- only uniformly distributed data

![image-20250209033907981](images/vertical_partition_scan.png)

**table creation**

```sql
CREATE TABLE account (
    id INT primary key, 
    balance text, 
    homeaddress text
);
CREATE TABLE account1 (
    id INT primary key, 
    balance text
);
CREATE TABLE account2 (
    id INT primary key, 
    homeaddress text
);
```

**query**

```sql
SELECT * FROM account;	
Select account1.id, balance, homeaddress from account1, account2 where account1.id = account2.id
SELECT id, balance FROM account;
SELECT id, balance FROM account1;
```





###  point query

tbc

![image-20250212110139460](images/vertical_partition_point.png)

several account with the same value on the column of %access?



##  View on Join

**data type**

- uniformly distributed data
- fractally distributed data

![image-20250209034608374](images/voj_settings.png)

**query**

1. create view

   ```sql
   create view techlocation as 
   select ssnum, techdept.dept, location 
   from employee, techdept 
   where employee.dept = techdept.dept;
   ```

2. queries

   with view on join

   ```sql
   select dept from techlocation where ssnum = 7891; -- becomes a join on employee, techdept.
   ```

   without view on join

   ```sql
   select dept from employee where ssnum = 7891;
   ```

**duckdb** table creation

seems cannot do explicit index

```sql
CREATE TABLE techdept (
    dept VARCHAR(25) PRIMARY KEY,  -- Clustered index
    manager VARCHAR(25),
    location VARCHAR(50)
);

CREATE TABLE employee (
    ssnum INT PRIMARY KEY,  -- Clustered index
    name VARCHAR(25),
    dept VARCHAR(25) references techdept(dept),
    salary DECIMAL(10,2),
    numfriends INT
);

CREATE TABLE student (
    ssnum INT PRIMARY KEY,  -- Clustered index
    name VARCHAR(50),
    course VARCHAR(50),
    grade INT
);

COPY employee FROM '/data/tw3090/dept/fractal7/employees_fractal_10_7.csv' WITH (HEADER TRUE, DELIMITER ',');

COPY techdept FROM '/data/tw3090/dept/fractal7/techdept_fractal_10_7.csv' WITH (HEADER TRUE, DELIMITER ',');

COPY student FROM '/data/tw3090/dept/fractal7/students_fractal_10_7.csv' WITH (HEADER TRUE, DELIMITER ',');

```

**mariadb** table creation

```sql
CREATE TABLE techdept (
    dept VARCHAR(25),  -- Clustered index
    manager VARCHAR(25),
    location VARCHAR(50)
);

CREATE TABLE employee (
    ssnum INT,  -- Clustered index (默认)
    name VARCHAR(25),
    dept VARCHAR(25),
    salary DECIMAL(10,2),
    numfriends INT
);

CREATE TABLE student (
    ssnum INT,  -- Clustered index
    name VARCHAR(50),
    course VARCHAR(50),
    grade INT
);

LOAD DATA LOCAL INFILE '/data/tw3090/dept/fractal5/employees_fractal_10_7.csv'
INTO TABLE employee
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


LOAD DATA LOCAL INFILE '/data/tw3090/dept/fractal5/techdept_fractal_10_7.csv'
INTO TABLE techdept
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


LOAD DATA LOCAL INFILE '/data/tw3090/dept/fractal5/students_fractal_10_7.csv'
INTO TABLE student
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


-- key
ALTER TABLE employee 
ADD PRIMARY KEY (ssnum);

ALTER TABLE student 
ADD PRIMARY KEY (ssnum);

ALTER TABLE techdept 
ADD PRIMARY KEY (dept);

ALTER TABLE employee 
ADD CONSTRAINT fk_employee_dept 
FOREIGN KEY (dept) REFERENCES techdept(dept);

-- index
CREATE UNIQUE CLUSTERED INDEX i1 ON employee (ssnum);  -- MySQL InnoDB default PK is Clustered
CREATE INDEX i2 ON employee (name);
CREATE INDEX i3 ON employee (dept);

CREATE UNIQUE CLUSTERED INDEX i4 ON student (ssnum);  -- MySQL InnoDB default PK is Clustered
CREATE INDEX i5 ON student (name);

CREATE UNIQUE CLUSTERED INDEX i6 ON techdept (dept);  -- MySQL InnoDB default PK is Clustered

```

##  correlated subqueries

**table**

use employee techdpt student

**data type**

- uniformly distributed data
- fractally distributed data

**query**

with correlated subqueries

```sql
select ssnum 
from employee e1 
where salary =
    (select max(salary)
     from employee e2
     where e2.dept = e1.dept);
```

rewritten

```sql
-- template, not using!!
select max(salary) as bigsalary, dept
into TEMP 
from employee group by dept;

select ssnum
from employee, TEMP
where salary = bigsalary
and employee.dept = temp.dept;

-- 1.
SELECT e1.ssnum
FROM employee e1
JOIN (
    SELECT dept, MAX(salary) AS bigsalary
    FROM employee
    GROUP BY dept
) e2 
ON e1.dept = e2.dept AND e1.salary = e2.bigsalary;

-- 2.
WITH max_salary_per_dept AS (
    SELECT dept, MAX(salary) AS bigsalary
    FROM employee
    GROUP BY dept
)
SELECT e1.ssnum
FROM employee e1
JOIN max_salary_per_dept m
ON e1.dept = m.dept AND e1.salary = m.bigsalary;

```

##  eliminate unneeded distinct

**table**

use employee techdpt student

**data type**

- uniformly distributed data
- fractally distributed data

**query**

without eliminating unneeded distinct

```sql
SELECT DISTINCT ssnum
FROM employee, tech
WHERE employee.dept = tech.dept

```

with eliminating unneeded distinct

```sql
SELECT ssnum
FROM employee, tech
WHERE employee.dept = tech.dept
```

##  Looping can hurt

**table**

use TPC_H dataset

![image-20250209033907981](images/looping_can_hurt_data.png)

**query**

![image-20250209033907981](images/looping_can_hurt_query.png)

No loop

```sql
SELECT * FROM lineitem WHERE l_partkey < 200;
```

Loop

```sql
PREPARE stmt FROM 'SELECT * FROM lineitem WHERE l_partkey = ?';

DELIMITER //
CREATE PROCEDURE get_lineitems()
BEGIN
    DECLARE i INT DEFAULT 1;
    
    WHILE i < 200 DO
        SET @param = i;
        EXECUTE stmt USING @param;
        SET i = i + 1;
    END WHILE;
    
END //
DELIMITER ;

CALL get_lineitems();
```
