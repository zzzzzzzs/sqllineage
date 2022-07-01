with t1 as (
    select id, name
    from (select id, name, age from info) a1
),
     t2 as (
         select id, age
         from (select id, name, age from foo) a2
     )
select name, age
from (
         select t1.name, t2.age
         from t1
                  left join t2 on t1.id = t2.id
     ) t;