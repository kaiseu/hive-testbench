drop table if exists partsupp;
create table partsupp
stored as ${var:FILE}
as select * from ${var:SOURCE}.partsupp
-- cluster by PS_SUPPKEY
;

