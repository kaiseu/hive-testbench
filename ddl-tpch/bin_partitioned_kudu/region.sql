drop table if exists region;
create table region
stored as ${var:FILE}
as select distinct * from ${var:SOURCE}.region;
