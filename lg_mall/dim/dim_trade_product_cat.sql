-- 商品分类表
drop table if exists dim_trade_product_cat;

create table dim_trade_product_cat
(
    first_id    int comment '一级商品分类id',
    first_name  string comment '一级商品分类名称',
    second_id   int comment '二级商品分类Id',
    second_name string comment '二级商品分类名称',
    third_id    int comment '三级商品分类id',
    third_name  string comment '三级商品分类名称'
) comment '商品分类表'
    partitioned by (dt string)
    stored as parquet;


-- 加载数据
set hive.execution.engine = spark;
set hivevar:do_date = 2020-07-12;
insert overwrite table dim_trade_product_cat
    partition (dt = '${do_date}')
select t1.cat_id,
       t1.cat_name,
       t2.cat_id,
       t2.cat_name,
       t3.cat_id,
       t3.cat_name
from (select cat_id,
             parent_id,
             cat_name
      from ods_trade_product_category
      where level = 3
        and dt = '${do_date}') t3
         left join
     (
         select cat_id,
                parent_id,
                cat_name
         from ods_trade_product_category
         where level = 2
           and dt = '${do_date}'
     ) t2
     on t3.parent_id = t2.cat_id
         left join (
    select cat_id,
           parent_id,
           cat_name
    from ods_trade_product_category
    where level = 1
      and dt = '${do_date}'
) t1 on t2.parent_id = t1.cat_id;



-- 查询数据
select first_id,
       first_name,
       second_id,
       second_name,
       third_id,
       third_name,
       dt
from dim_trade_product_cat
where dt = '${do_date}'
limit 10;


