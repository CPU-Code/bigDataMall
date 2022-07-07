-- 退单表
drop table if exists ods_order_refund_info_inc;

create external table ods_order_refund_info_inc
(
    `type` string comment '变动类型',
    `ts`   bigint comment '变动时间',
    `data` struct<id :string,user_id :string,order_id :string,sku_id :string,refund_type :string,refund_num :bigint,refund_amount
                  :decimal(16, 2),refund_reason_type :string,refund_reason_txt :string,refund_status :string,create_time
                  :string> comment '数据',
    `old`  map<string,string> comment '旧值'
) comment '退单表'
    partitioned by (`dt` string)
    row format serde 'org.apache.hadoop.hive.serde2.JsonSerDe'
    location '/warehouse/gmall/ods/ods_order_refund_info_inc/';

-- 装载数据
set hivevar:do_date = 2020-06-14;
load data inpath '/origin_data/gmall/db/order_refund_info_inc/${do_date}'
    into table ods_order_refund_info_inc
    partition (dt = '${do_date}');


-- 查询数据
select type,
       ts,
       data.id,
       data.user_id,
       data.order_id,
       data.sku_id,
       data.refund_type,
       data.refund_num,
       data.refund_amount,
       data.refund_reason_type,
       data.refund_reason_txt,
       data.refund_status,
       data.create_time,
       old,
       dt
from ods_order_refund_info_inc
where dt = '${do_date}'
limit 10;

