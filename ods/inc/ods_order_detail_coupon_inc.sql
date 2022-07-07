-- 创建 订单明细优惠券关联表
drop table if exists ods_order_detail_coupon_inc;

create external table ods_order_detail_coupon_inc
(
    `type` string comment '变动类型',
    `ts`   bigint comment '变动时间',
    `data` struct<id :string, order_id :string, order_detail_id :string, coupon_id :string, coupon_use_id :string,
                  sku_id :string, create_time :string> comment '数据',
    `old`  map<string,string> comment '旧值'
) comment '订单明细优惠券关联表'
    partitioned by (`dt` string)
    row format serde 'org.apache.hadoop.hive.serde2.JsonSerDe'
    location '/warehouse/gmall/ods/ods_order_detail_coupon_inc/';


-- 装载数据
set hivevar:do_date = 2020-06-14;
load data inpath '/origin_data/gmall/db/order_detail_coupon_inc/${do_date}'
    into table ods_order_detail_coupon_inc
    partition (dt = '${do_date}');


-- 查询数据
select type,
       ts,
       data.id,
       data.order_id,
       data.order_detail_id,
       data.coupon_id,
       data.coupon_use_id,
       data.sku_id,
       data.create_time,
       old
from ods_order_detail_coupon_inc
where dt = '${do_date}';

