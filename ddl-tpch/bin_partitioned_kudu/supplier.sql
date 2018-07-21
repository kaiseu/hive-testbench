drop table if exists supplier;
create table supplier
stored as ${var:FILE}
as select * from ${var:SOURCE}.supplier
-- cluster by s_nationkey, s_suppkey
;
