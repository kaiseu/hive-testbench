drop table if exists nation;
create table nation
stored as ${var:FILE}
as select distinct * from ${var:SOURCE}.nation;
