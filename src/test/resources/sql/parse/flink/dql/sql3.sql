select
    avg(col),
    agg_func(col),
    key
from
    t1
    join t2 on t1.fk1 = t2.fk2
group by
    key
having
    avg(col) > 5