with t1 as (
    select name
    from info1
),
     t2 as (
         select age
         from info2
     )
select age
from t2;