-- 创建 订单明细表
drop table if exists ods_order_detail_inc;

create external table ods_order_detail_inc
(
    `type` string comment '变动类型',
    `ts`   bigint comment '变动时间',
    `data` struct<id :string, order_id :string, sku_id :string, sku_name :string, img_url :string, order_price
                  :decimal(16, 2), sku_num :bigint, create_time :string, source_type :string, source_id :string,
                  split_total_amount :decimal(16, 2), split_activity_amount :decimal(16, 2), split_coupon_amount
                  :decimal(16, 2)> comment '数据',
    `old`  map<string,string> comment '旧值'
) comment '订单明细表'
    partitioned by (`dt` string)
    row format serde 'org.apache.hadoop.hive.serde2.JsonSerDe'
    location '/warehouse/gmall/ods/ods_order_detail_inc/';


-- 装载数据
set hivevar:do_date = 2020-06-14;
load data inpath '/origin_data/gmall/db/order_detail_inc/${do_date}'
    into table ods_order_detail_inc
    partition (dt = '${do_date}');


-- 查询数据
set hivevar:do_date = 2020-06-14;
select type,
       ts,
       data.id,
       data.order_id,
       data.sku_id,
       data.sku_name,
       data.img_url,
       data.order_price,
       data.sku_num,
       data.create_time,
       data.source_type,
       data.source_id,
       data.split_total_amount,
       data.split_activity_amount,
       data.split_coupon_amount,
       old,
       dt
from ods_order_detail_inc
where dt = '${do_date}'
limit 10;