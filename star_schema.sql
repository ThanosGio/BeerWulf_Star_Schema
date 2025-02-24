Create database BeerWulf;
use BeerWulf;

CREATE TABLE NATION (
  N_NATIONKEY INTEGER PRIMARY KEY NOT NULL,
  N_NAME      TEXT NOT NULL,
  N_REGIONKEY INTEGER NOT NULL,
  N_COMMENT   TEXT,
  FOREIGN KEY (N_REGIONKEY) REFERENCES REGION(R_REGIONKEY)
);

CREATE TABLE REGION (
  R_REGIONKEY INTEGER PRIMARY KEY NOT NULL,
  R_NAME      TEXT NOT NULL,
  R_COMMENT   TEXT
);

CREATE TABLE PART (
  P_PARTKEY     INTEGER PRIMARY KEY NOT NULL,
  P_NAME        TEXT NOT NULL,
  P_MFGR        TEXT NOT NULL,
  P_BRAND       TEXT NOT NULL,
  P_TYPE        TEXT NOT NULL,
  P_SIZE        INTEGER NOT NULL,
  P_CONTAINER   TEXT NOT NULL,
  P_RETAILPRICE INTEGER NOT NULL,
  P_COMMENT     TEXT NOT NULL
);

CREATE TABLE SUPPLIER (
  S_SUPPKEY   INTEGER PRIMARY KEY NOT NULL,
  S_NAME      TEXT NOT NULL,
  S_ADDRESS   TEXT NOT NULL,
  S_NATIONKEY INTEGER NOT NULL,
  S_PHONE     TEXT NOT NULL,
  S_ACCTBAL   INTEGER NOT NULL,
  S_COMMENT   TEXT NOT NULL,
  FOREIGN KEY (S_NATIONKEY) REFERENCES NATION(N_NATIONKEY)
);

CREATE TABLE PARTSUPP (
  PS_PARTKEY    INTEGER NOT NULL,
  PS_SUPPKEY    INTEGER NOT NULL,
  PS_AVAILQTY   INTEGER NOT NULL,
  PS_SUPPLYCOST INTEGER NOT NULL,
  PS_COMMENT    TEXT NOT NULL,
  PRIMARY KEY (PS_PARTKEY, PS_SUPPKEY),
  FOREIGN KEY (PS_SUPPKEY) REFERENCES SUPPLIER(S_SUPPKEY),
  FOREIGN KEY (PS_PARTKEY) REFERENCES PART(P_PARTKEY)
);

CREATE TABLE CUSTOMER (
  C_CUSTKEY    INTEGER PRIMARY KEY NOT NULL,
  C_NAME       TEXT NOT NULL,
  C_ADDRESS    TEXT NOT NULL,
  C_NATIONKEY  INTEGER NOT NULL,
  C_PHONE      TEXT NOT NULL,
  C_ACCTBAL    INTEGER   NOT NULL,
  C_MKTSEGMENT TEXT NOT NULL,
  C_COMMENT    TEXT NOT NULL,
  FOREIGN KEY (C_NATIONKEY) REFERENCES NATION(N_NATIONKEY)
);

CREATE TABLE ORDERS (
  O_ORDERKEY      INTEGER PRIMARY KEY NOT NULL,
  O_CUSTKEY       INTEGER NOT NULL,
  O_ORDERSTATUS   TEXT NOT NULL,
  O_TOTALPRICE    INTEGER NOT NULL,
  O_ORDERDATE     DATE NOT NULL,
  O_ORDERPRIORITY TEXT NOT NULL,  
  O_CLERK         TEXT NOT NULL, 
  O_SHIPPRIORITY  INTEGER NOT NULL,
  O_COMMENT       TEXT NOT NULL,
  FOREIGN KEY (O_CUSTKEY) REFERENCES CUSTOMER(C_CUSTKEY)
);

CREATE TABLE LINEITEM (
  L_ORDERKEY      INTEGER NOT NULL,
  L_PARTKEY       INTEGER NOT NULL,
  L_SUPPKEY       INTEGER NOT NULL,
  L_LINENUMBER    INTEGER NOT NULL,
  L_QUANTITY      INTEGER NOT NULL,
  L_EXTENDEDPRICE INTEGER NOT NULL,
  L_DISCOUNT      INTEGER NOT NULL,
  L_TAX           INTEGER NOT NULL,
  L_RETURNFLAG    TEXT NOT NULL,
  L_LINESTATUS    TEXT NOT NULL,
  L_SHIPDATE      DATE NOT NULL,
  L_COMMITDATE    DATE NOT NULL,
  L_RECEIPTDATE   DATE NOT NULL,
  L_SHIPINSTRUCT  TEXT NOT NULL,
  L_SHIPMODE      TEXT NOT NULL,
  L_COMMENT       TEXT NOT NULL,
  PRIMARY KEY (L_ORDERKEY, L_LINENUMBER),
  FOREIGN KEY (L_ORDERKEY) REFERENCES ORDERS(O_ORDERKEY),
  FOREIGN KEY (L_PARTKEY, L_SUPPKEY) REFERENCES PARTSUPP(PS_PARTKEY, PS_SUPPKEY)
);
SHOW GLOBAL VARIABLES LIKE 'local_infile';
SET GLOBAL local_infile=1;
SET PERSIST local_infile = 1;
SET SESSION local_infile = 1;
SHOW GLOBAL VARIABLES LIKE 'local_infile';
SHOW VARIABLES LIKE 'secure_file_priv';

-- 1 region
LOAD DATA LOCAL INFILE 'C:/github/BeerWulf_Star_Schema/data/region.tbl'
INTO TABLE REGION
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
(R_REGIONKEY, R_NAME, R_COMMENT);


-- 2 nation
LOAD DATA LOCAL INFILE 'C:/github/BeerWulf_Star_Schema/data/nation.tbl'
INTO TABLE NATION
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
(
  N_NATIONKEY,
  N_NAME,
  N_REGIONKEY,
  N_COMMENT
  );
  
-- 3. PART
LOAD DATA LOCAL INFILE 'C:/github/BeerWulf_Star_Schema/data/part.tbl'
INTO TABLE PART
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
(P_PARTKEY, P_NAME, P_MFGR, P_BRAND, P_TYPE,
 P_SIZE, P_CONTAINER, P_RETAILPRICE, P_COMMENT);

-- 4. SUPPLIER
LOAD DATA LOCAL INFILE 'C:/github/BeerWulf_Star_Schema/data/supplier.tbl'
INTO TABLE SUPPLIER
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
(S_SUPPKEY, S_NAME, S_ADDRESS, S_NATIONKEY, S_PHONE,
 S_ACCTBAL, S_COMMENT);

-- 5. PARTSUPP
LOAD DATA LOCAL INFILE 'C:/github/BeerWulf_Star_Schema/data/partsupp.tbl'
INTO TABLE PARTSUPP
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
(PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY, PS_SUPPLYCOST, PS_COMMENT);

-- 6. CUSTOMER
LOAD DATA LOCAL INFILE 'C:/github/BeerWulf_Star_Schema/data/customer.tbl'
INTO TABLE CUSTOMER
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
(C_CUSTKEY, C_NAME, C_ADDRESS, C_NATIONKEY, C_PHONE,
 C_ACCTBAL, C_MKTSEGMENT, C_COMMENT);

-- 7. ORDERS
LOAD DATA LOCAL INFILE 'C:/github/BeerWulf_Star_Schema/data/orders.tbl'
INTO TABLE ORDERS
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
(O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE, O_ORDERDATE,
 O_ORDERPRIORITY, O_CLERK, O_SHIPPRIORITY, O_COMMENT);

-- 8. LINEITEM
LOAD DATA LOCAL INFILE 'C:/github/BeerWulf_Star_Schema/data/lineitem.tbl'
INTO TABLE LINEITEM
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
(L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER,
 L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX,
 L_RETURNFLAG, L_LINESTATUS,
 L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE,
 L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT);  
 
 
 -- Create start schema.
 
-- dim date 
 
CREATE TABLE dim_date (
  date_key    INT AUTO_INCREMENT PRIMARY KEY,  -- Surrogate key (auto-increment)
  full_date   DATE NOT NULL,
  day_of_week INT,
  month       INT,
  quarter     INT,
  year        INT
  -- You can add more date attributes (e.g. day_name, is_weekend, etc.)
);


-- populate date
INSERT INTO dim_date (full_date, day_of_week, month, quarter, year)
SELECT DISTINCT d.dt AS full_date,
       DAYOFWEEK(d.dt)  AS day_of_week,
       MONTH(d.dt)      AS month,
       QUARTER(d.dt)    AS quarter,
       YEAR(d.dt)       AS year
FROM (
  SELECT o_orderdate AS dt
    FROM orders
  UNION
  SELECT l_shipdate
    FROM lineitem
  UNION
  SELECT l_commitdate
    FROM lineitem
  UNION
  SELECT l_receiptdate
    FROM lineitem
) AS d
WHERE d.dt IS NOT NULL
ORDER BY d.dt;

 
-- dim customer

CREATE TABLE dim_customer (
  customer_key  INTEGER  NOT NULL PRIMARY KEY,  -- Surrogate key for the dimension
  c_custkey     INTEGER  NOT NULL,             -- Original OLTP key (optional, for reference)

  c_name        TEXT,
  c_address     TEXT,
  c_phone       TEXT,
  c_acctbal     INTEGER,
  c_mktsegment  TEXT,
  c_comment     TEXT,

  nation_name   TEXT,   -- Flattened from NATION
  region_name   TEXT    -- Flattened from REGION
);

-- insert data into dim_customer

 INSERT INTO dim_customer (
  customer_key, c_custkey, c_name, c_address, c_phone, c_acctbal,
  c_mktsegment, c_comment, nation_name, region_name
)
SELECT
  -- generate a surrogate key (e.g. via sequence or auto-increment):
  ROW_NUMBER() OVER (ORDER BY c.c_custkey) AS customer_key,
  c.c_custkey,
  c.c_name,
  c.c_address,
  c.c_phone,
  c.c_acctbal,
  c.c_mktsegment,
  c.c_comment,
  n.n_name      AS nation_name,
  r.r_name      AS region_name
FROM CUSTOMER c
JOIN NATION   n ON c.c_nationkey = n.n_nationkey
JOIN REGION   r ON n.n_regionkey = r.r_regionkey;

CREATE TABLE dim_supplier (
  supplier_key INT AUTO_INCREMENT PRIMARY KEY,  -- Surrogate key
  s_suppkey    INT NOT NULL,                    -- Original OLTP key, if you want to keep it
  
  s_name       TEXT,
  s_address    TEXT,
  s_phone      TEXT,
  s_acctbal    INT,
  s_comment    TEXT,
  
  nation_name  TEXT,
  region_name  TEXT
  -- add more descriptive fields if you like
);

INSERT INTO dim_supplier (
  s_suppkey,
  s_name,
  s_address,
  s_phone,
  s_acctbal,
  s_comment,
  nation_name,
  region_name
)
SELECT 
  s.s_suppkey,
  s.s_name,
  s.s_address,
  s.s_phone,
  s.s_acctbal,
  s.s_comment,
  n.n_name AS nation_name,
  r.r_name AS region_name
FROM SUPPLIER s
JOIN NATION   n ON s.s_nationkey = n.n_nationkey
JOIN REGION   r ON n.n_regionkey = r.r_regionkey;


 


CREATE TABLE dim_part (
  part_key     INT AUTO_INCREMENT PRIMARY KEY, -- Surrogate key
  p_partkey    INT NOT NULL,                  -- Original OLTP key
  
  p_name       TEXT,
  p_mfgr       TEXT,
  p_brand      TEXT,
  p_type       TEXT,
  p_size       INT,
  p_container  TEXT,
  p_retailprice INT,
  p_comment    TEXT
  -- add more columns if needed
);

INSERT INTO dim_part (
  p_partkey,
  p_name,
  p_mfgr,
  p_brand,
  p_type,
  p_size,
  p_container,
  p_retailprice,
  p_comment
)
SELECT
  p_partkey,
  p_name,
  p_mfgr,
  p_brand,
  p_type,
  p_size,
  p_container,
  p_retailprice,
  p_comment
FROM PART;



-- create fact table

CREATE TABLE fact_lineitem (
  fact_lineitem_key  INTEGER   NOT NULL PRIMARY KEY, -- Surrogate key for the fact row

  -- Foreign keys to dimension tables:
  customer_key       INTEGER   NOT NULL,  -- from dim_customer
  supplier_key       INTEGER   NOT NULL,  -- from dim_supplier
  part_key           INTEGER   NOT NULL,  -- from dim_part
  order_date_key     INTEGER   NOT NULL,  -- from dim_date (o_orderdate)
  ship_date_key      INTEGER   NOT NULL,  -- from dim_date (l_shipdate)
  commit_date_key    INTEGER   NOT NULL,  -- from dim_date (l_commitdate)
  receipt_date_key   INTEGER   NOT NULL,  -- from dim_date (l_receiptdate)
  
  -- Measures and textual flags from lineitem (and possibly orders):
  l_quantity         INTEGER   NOT NULL,
  l_extendedprice    INTEGER   NOT NULL,
  l_discount         INTEGER   NOT NULL,
  l_tax              INTEGER   NOT NULL,

  o_orderstatus      TEXT,
  o_shippriority     INTEGER,
  
  l_returnflag       TEXT      NOT NULL,
  l_linestatus       TEXT      NOT NULL,

  -- You can include more fields if they're useful metrics or partition columns
  -- e.g., O_TOTALPRICE (aggregated measure from the order level),
  -- or line_number for uniqueness/troubleshooting
  l_linenumber       INTEGER   NOT NULL
  -- ...
);

-- Populate fact table
INSERT INTO fact_lineitem (
  fact_lineitem_key, 
  customer_key, supplier_key, part_key,
  order_date_key, ship_date_key, commit_date_key, receipt_date_key,
  l_quantity, l_extendedprice, l_discount, l_tax,
  o_orderstatus, o_shippriority,
  l_returnflag, l_linestatus,
  l_linenumber
)
SELECT
  ROW_NUMBER() OVER (ORDER BY l.l_orderkey, l.l_linenumber) AS fact_lineitem_key,
  dc.customer_key,
  ds.supplier_key,
  dp.part_key,
  dd_order.date_key      AS order_date_key,
  dd_ship.date_key       AS ship_date_key,
  dd_commit.date_key     AS commit_date_key,
  dd_receipt.date_key    AS receipt_date_key,
  
  l.l_quantity,
  l.l_extendedprice,
  l.l_discount,
  l.l_tax,

  o.o_orderstatus,
  o.o_shippriority,

  l.l_returnflag,
  l.l_linestatus,
  l.l_linenumber

FROM LINEITEM l
JOIN ORDERS   o ON l.l_orderkey = o.o_orderkey
JOIN CUSTOMER c ON o.o_custkey  = c.c_custkey
JOIN SUPPLIER s ON (l.l_suppkey = s.s_suppkey)
JOIN PART     p ON l.l_partkey  = p.p_partkey
JOIN NATION   nc ON c.c_nationkey = nc.n_nationkey
JOIN REGION   rc ON nc.n_regionkey = rc.r_regionkey
JOIN NATION   ns ON s.s_nationkey = ns.n_nationkey
JOIN REGION   rs ON ns.n_regionkey = rs.r_regionkey
-- lookups for dimension surrogate keys:
JOIN dim_customer dc ON dc.c_custkey = c.c_custkey
                      AND dc.nation_name = nc.n_name
                      AND dc.region_name = rc.r_name
JOIN dim_supplier ds ON ds.s_suppkey = s.s_suppkey
                      AND ds.nation_name = ns.n_name
                      AND ds.region_name = rs.r_name
JOIN dim_part     dp ON dp.p_partkey = p.p_partkey
-- date lookups:
JOIN dim_date dd_order    ON dd_order.full_date    = o.o_orderdate
JOIN dim_date dd_ship     ON dd_ship.full_date     = l.l_shipdate
JOIN dim_date dd_commit   ON dd_commit.full_date   = l.l_commitdate
JOIN dim_date dd_receipt  ON dd_receipt.full_date  = l.l_receiptdate
ORDER BY l.l_orderkey, l.l_linenumber;




 