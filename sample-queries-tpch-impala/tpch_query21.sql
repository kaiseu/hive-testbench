-- explain
--drop table if exists l3;
-- create table l3 stored as PARQUET as 
drop view if exists l3;
create view l3 as 
-- select l_orderkey, count(distinct l_suppkey) as cntSupp
select l_orderkey, count(distinct l_suppkey)
from lineitem
where l_receiptdate > l_commitdate and l_orderkey is not null
group by l_orderkey
having count(distinct l_suppkey) = 1
--having cntSupp = 1
;

with location123 as (
select supplier.* from supplier, nation where
s_nationkey = n_nationkey and n_name = 'SAUDI ARABIA'
)
select s_name, count(*) as numwait
from
(
select li.l_suppkey, li.l_orderkey
from lineitem li join orders o on li.l_orderkey = o.o_orderkey and
                      o.o_orderstatus = 'F'
     join
     (
     select l_orderkey, count(distinct l_suppkey) as cntSupp
     -- select l_orderkey, count(distinct l_suppkey)
     from lineitem
     group by l_orderkey
     ) l2 on li.l_orderkey = l2.l_orderkey and 
             li.l_receiptdate > li.l_commitdate and 
             l2.cntSupp > 1
) l1 join l3 on l1.l_orderkey = l3.l_orderkey
 join location123 s on l1.l_suppkey = s.s_suppkey
group by
 s_name
order by
 numwait desc,
 s_name
limit 100;
profile;
