drop table if exists customer;
create table customer
stored as ${var:FORMAT}
as select * from ${var:SOURCE_DB}.customer
-- cluster by C_MKTSEGMENT
;

drop table if exists lineitem;
create table lineitem 
(L_ORDERKEY BIGINT,
 L_PARTKEY BIGINT,
 L_SUPPKEY BIGINT,
 L_LINENUMBER INT,
 L_QUANTITY DOUBLE,
 L_EXTENDEDPRICE DOUBLE,
 L_DISCOUNT DOUBLE,
 L_TAX DOUBLE,
 L_RETURNFLAG STRING,
 L_LINESTATUS STRING,
 L_COMMITDATE STRING,
 L_RECEIPTDATE STRING,
 L_SHIPINSTRUCT STRING,
 L_SHIPMODE STRING,
 L_COMMENT STRING)
 partitioned by (L_SHIPDATE STRING)
stored as ${var:FORMAT}
;

INSERT OVERWRITE TABLE lineitem Partition(L_SHIPDATE)
select 
L_ORDERKEY ,
 L_PARTKEY ,
 L_SUPPKEY ,
 L_LINENUMBER ,
 L_QUANTITY ,
 L_EXTENDEDPRICE ,
 L_DISCOUNT ,
 L_TAX ,
 L_RETURNFLAG ,
 L_LINESTATUS ,
 L_COMMITDATE ,
 L_RECEIPTDATE ,
 L_SHIPINSTRUCT ,
 L_SHIPMODE ,
 L_COMMENT ,
 L_SHIPDATE
 from ${var:SOURCE_DB}.lineitem
;

drop table if exists nation;
create table nation
stored as ${var:FORMAT}
as select distinct * from ${var:SOURCE_DB}.nation;
drop table if exists orders;
create table orders (O_ORDERKEY BIGINT,
 O_CUSTKEY BIGINT,
 O_ORDERSTATUS STRING,
 O_TOTALPRICE DOUBLE,
 O_ORDERPRIORITY STRING,
 O_CLERK STRING,
 O_SHIPPRIORITY INT,
 O_COMMENT STRING)
 partitioned by (O_ORDERDATE STRING)
stored as ${var:FORMAT}
;

INSERT OVERWRITE TABLE orders partition(O_ORDERDATE)
select 
O_ORDERKEY ,
 O_CUSTKEY ,
 O_ORDERSTATUS ,
 O_TOTALPRICE ,
 O_ORDERPRIORITY ,
 O_CLERK ,
 O_SHIPPRIORITY ,
 O_COMMENT,
 O_ORDERDATE
  from ${var:SOURCE_DB}.orders
;


drop table if exists part;
create table part
stored as ${var:FORMAT}
as select * from ${var:SOURCE_DB}.part
-- cluster by p_brand
;
drop table if exists partsupp;
create table partsupp
stored as ${var:FORMAT}
as select * from ${var:SOURCE_DB}.partsupp
-- cluster by PS_SUPPKEY
;

drop table if exists region;
create table region
stored as ${var:FORMAT}
as select distinct * from ${var:SOURCE_DB}.region;
drop table if exists supplier;
create table supplier
stored as ${var:FORMAT}
as select * from ${var:SOURCE_DB}.supplier
-- cluster by s_nationkey, s_suppkey
;
