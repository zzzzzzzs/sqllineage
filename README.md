# 介绍

本项目是一个不依赖元数据的 sql 解析器，将最后解析的 sql 血缘关系图展示在页面上。

# 使用说明

在文本框中输入 sql 语句，然后点击渲染接口数据，就可以看到 sql 血缘关系图。

例如，在文本框输入下面的 sql 语句
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

# 解析过程

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



# 有限状态机

<details>   <summary>折叠文本</summary>   此处可书写文本   嗯，是可以书写文本的 </details>


![Alt text](https://g.gravizo.com/g?
digraph G {
    rankdir = TB;
    size = "8,5"
    node [shape = box];
    INIT [label="INITIAL"];
    TABLE [label="TABLE"];
    JOIN [label="JOIN"];
    REAL [label="REAL"];
    REST [label="REST"];
    RESC [label="RESC"];
    INIT -> TABLE [ label = "table"];
    TABLE -> REST [ label = "real"];
    TABLE -> REAL [ label = "real"];
    REAL -> REST [ label = "as"];
    TABLE -> JOIN [ label = "join"];
    JOIN -> REST [ label = "real"];
    INIT -> COLUMN [ label = "column"]
    COLUMN -> RESC [label = "real"]
    COLUMN -> REAL [label = "real"]
    REAL -> RESC [label = "as"]
}
)


# 注意

- 使用 Java17
- 尽量将字段写全
- 子函数要写别名
- 目前是在后端处理的坐标，后期需要在前端处理

# 待解决问题

- [ ] 支持 * ，通过谓词，排序，join 条件等反馈补全的表名
- [ ] queryxxx.sql 都需要测试通过
- [ ] 目前不支持 join 操作
- [ ] 后期可以搞一个数据结构将没有表名，但有列名和.的获取到补全列名 (orderby 和 group by)
- [ ] 高并发下有问题
- [ ] 在前端处理坐标

# 参考
- 本项目的前端使用了 [此](https://github.com/mizuhokaga/jsplumb-dataLineage-vue) 项目。
- http://jsfiddle.net/rayflex/La9p4/

