insert
into
    tableA
select
    t.id as id,
    s1.name as name,
    s2.msg as msg
from
    tableB t
        left join
    tableC s1
    on t.id = s1.id
        left join
    tableD s2
    on t.id = s2.id