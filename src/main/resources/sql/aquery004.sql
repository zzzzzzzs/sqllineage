select age, name
from (
         select age, name, class
         from (
                  select age, name, class, grade
                  from (
                           select age, name, class, grade, gender
                           from data1
                       ) t1
              ) t2
     ) t3;