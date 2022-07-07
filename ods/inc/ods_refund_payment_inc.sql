-- 退款表
drop table if exists ods_refund_payment_inc;

create external table ods_refund_payment_inc
(
    `type` string comment '变动类型',
    `ts`   bigint comment '变动时间',
    `data` struct<id :string,out_trade_no :string,order_id :string,sku_id :string,payment_type :STRING,trade_no :string,total_amount
                  :decimal(16, 2),subject :string,refund_status :string,create_time :string,callback_time :string,callback_content
                  :string> comment '数据',
    `old`  map<string,string> comment '旧值'
) comment '退款表'
    partitioned by (`dt` string)
    row format serde 'org.apache.hadoop.hive.serde2.JsonSerDe'
    location '/warehouse/gmall/ods/ods_refund_payment_inc/';

-- 装载数据
set hivevar:do_date = 2020-06-14;
load data inpath '/origin_data/gmall/db/refund_payment_inc/${do_date}'
    into table ods_refund_payment_inc
    partition (dt = '${do_date}');


-- 查询数据
select type,
       ts,
       data.id,
       data.out_trade_no,
       data.order_id,
       data.sku_id,
       data.payment_type,
       data.trade_no,
       data.total_amount,
       data.subject,
       data.refund_status,
       data.create_time,
       data.callback_time,
       data.callback_content,
       old,
       dt
from ods_refund_payment_inc
where dt = '${do_date}'
limit 10;