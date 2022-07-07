-- 创建订单表
drop table if exists ods_order_info_inc;

create external table ods_order_info_inc
(
    `type` string comment '变动类型',
    `ts`   bigint comment '变动时间',
    `data` struct<id, data.consignee, data.consignee_tel, data.total_amount, data.order_status, data.user_id, data.
                  payment_way,delivery_address, data.order_comment, out_trade_no, data.trade_body, data.create_time,operate_time,
                  data.expire_time, data.process_status, data.tracking_no, data.parent_order_id, data.img_url,
                  province_id, data.activity_reduce_amount, data.coupon_reduce_amount, original_total_amount, data.
                  freight_fee, data.freight_fee_reduce, data.refundable_time> comment '数据',
    `old`  map<string,string> comment '旧值'
) comment '订单表'
    partitioned by (`dt` string)
    row format serde 'org.apache.hadoop.hive.serde2.JsonSerDe'
    location '/warehouse/gmall/ods/ods_order_info_inc/';


-- 装载数据
set hivevar:do_date = 2020-06-14;
load data inpath '/origin_data/gmall/db/order_info_inc/${do_date}'
    into table ods_order_info_inc
    partition (dt = '${do_date}');


-- 查询数据
set hivevar:do_date = 2020-06-14;
select type,
       ts,
       data.id,
       data.consignee,
       data.consignee_tel,
       data.total_amount,
       data.order_status,
       data.user_id,
       data.payment_way,
       data.delivery_address,
       data.order_comment,
       data.out_trade_no,
       data.trade_body,
       data.create_time,
       data.operate_time,
       data.expire_time,
       data.process_status,
       data.tracking_no,
       data.parent_order_id,
       data.img_url,
       data.province_id,
       data.activity_reduce_amount,
       data.coupon_reduce_amount,
       data.original_total_amount,
       data.freight_fee,
       data.freight_fee_reduce,
       data.refundable_time,
       old,
       dt
from ods_order_info_inc
where dt = '${do_date}'
limit 10;
