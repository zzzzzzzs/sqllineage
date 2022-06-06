# sqllineage

# target

Parse the SQL and get lineage, and finally display it visually.

Parse sql colum is belong to  table.

# features
```sql
select a.ca_state state, count(*) cnt
from customer_address a
   , customer c
   , store_sales s
   , date_dim d
   , item i
where a.ca_address_sk = c.c_current_addr_sk
  and c.c_customer_sk = s.ss_customer_sk
  and s.ss_sold_date_sk = d.d_date_sk
  and s.ss_item_sk = i.i_item_sk
  and d.d_month_seq =
      (select distinct (d_month_seq)
       from date_dim
       where d_year = 2001
         and d_moy = 1)
  and i.i_current_price > 1.2 *
                          (select avg(j.i_current_price)
                           from item j
                           where j.i_category = i.i_category)
group by a.ca_state
having count(*) >= 10
order by cnt
limit 100;
```

Binary tree

![Alt text](https://g.gravizo.com/g?
digraph G {
    node [shape=box];
    Source[label="Source"]
    SelectList[label="SelectList"]
    From[label="From"]
    Where[label="Where"]
    SqlBasicCallA[label="SqlBasicCall"]
    SqlBasicCallB[label="SqlBasicCall"]
    Left[label="Left"]
    Right[label="Right"]
    LeftA[label="Left"]
    SqlBasicCallC[label="SqlBasicCall"]
    SqlBasicCallD[label="SqlBasicCall"]
    SqlBasicCallE[label="SqlBasicCall"]
    Source->SelectList
    Source->From
    Source->Where
    Source->OrderBy
    SelectList->SqlBasicCallA
    SelectList->SqlBasicCallB
    From->Left
    From->Right
    Left->LeftA->SqlBasicCallC
    Left->RightB->SqlBasicCallD
    Right->SqlBasicCallE
}
)


# 注意

- [ ] 目前为止，子函数必须有别名
- [ ] 目前为止，不支持 * 反馈补前面的表名
