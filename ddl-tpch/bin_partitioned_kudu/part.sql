drop table if exists part;
create table part
stored as ${var:FILE}
as select * from ${var:SOURCE}.part
-- cluster by p_brand
;
