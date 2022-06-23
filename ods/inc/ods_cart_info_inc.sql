-- 删除购物车表
drop table if exists ods_cart_info_inc;

-- 创建 购物车表
create external table ods_cart_info_inc
(
    `type` string comment '变动类型',
    `ts`   bigint comment '变动时间',
    `data` struct<id :string, user_id :string,sku_id :string, cart_price :decimal(16, 2), sku_num :bigint,img_url
                  :string,sku_name :string,is_checked :string,create_time :string, operate_time :string,is_ordered
                  :string,order_time :string,source_type :string,source_id :string> comment '数据',
    `old`  map<string,string> comment '旧值'
) comment '购物车增量表'
    partitioned by (`dt` string)
    row format serde 'org.apache.hadoop.hive.serde2.JsonSerDe'
    location '/warehouse/gmall/ods/ods_cart_info_inc/';

-- 转载数据
load data inpath '/origin_data/gmall/db/cart_info_inc/2020-06-14'
    overwrite into table ods_cart_info_inc
    partition (dt = '2020-06-14');

-- 查询数据
set hive.execution.engine = spark;
select type,
       ts,
       data.id,
       data.user_id,
       data.sku_id,
       data.create_time,
       data.source_id,
       data.source_type,
       data.sku_num,
       old['id'] as old_id,
       old['user_id'] as old_user_id,
       old['sku_id'] as old_sku_id,
       old['create_time'] as old_sku_id,
       old['source_id'] as old_source_id,
       old['source_type'] as old_source_type,
       old['sku_num'] as old_sku_num,
       dt
from ods_cart_info_inc
where dt = '2020-06-14'
limit 100;