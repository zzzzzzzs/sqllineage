select name  as name_a,
       age   as age_a,
       grade as grade_a
from (select name, age, grade from info) t1
