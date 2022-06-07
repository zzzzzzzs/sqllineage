with t1 as (
    select name, age
    from info1
),
     t2 as (
         select name
         from t1
     )
select age
from t2;