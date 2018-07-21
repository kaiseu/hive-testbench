drop table if exists customer;
create table customer
stored as ${var:FILE}
as select * from ${var:SOURCE}.customer
-- cluster by C_MKTSEGMENT
;

