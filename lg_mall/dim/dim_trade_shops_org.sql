-- 商品地域组织表
drop table if exists dim_trade_shops_org;

create table dim_trade_shops_org
(
    shop_id     int comment '商家id',
    shop_name   string comment '商家名',
    city_id     int comment '城市id',
    city_name   string comment '城市名',
    region_id   int comment '地区id',
    region_name string comment '地区名'
) comment '商品地域组织表'
    partitioned by (dt string)
    stored as parquet;


-- 加载数据
set hive.execution.engine = spark;
set hivevar:do_date = 2020-07-12;
insert overwrite table dim_trade_shops_org
    partition (dt = '${do_date}')
select t1.shop_id,
       t1.shop_name,
       t2.id       city_id,
       t2.org_name city_name,
       t3.id       region_id,
       t3.org_name region_name
from (select shop_id,
             shop_name,
             area_id
      from ods_trade_shops
      where dt = '${do_date}') t1
         left join
     (select id,
             org_name,
             parent_id
      from ods_trade_shop_admin_org
      where org_level = 2
        and dt = '${do_date}') t2
     on t1.area_id = t2.id
         left join
     (select id,
             org_name
      from ods_trade_shop_admin_org
      where org_level = 1
        and dt = '${do_date}') t3
     on t2.parent_id = t3.id;


-- 查询数据
select shop_id,
       shop_name,
       city_id,
       city_name,
       region_id,
       region_name,
       dt
from dim_trade_shops_org
where dt = '${do_date}'
limit 10;
