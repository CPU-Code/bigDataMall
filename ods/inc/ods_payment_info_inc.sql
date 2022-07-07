-- 查询支付表
drop table if exists ods_payment_info_inc;

create external table ods_payment_info_inc
(
    `type` string comment '变动类型',
    `ts`   bigint comment '变动时间',
    `data` struct<id :string,out_trade_no :string,order_id :string,user_id :string,payment_type :string,trade_no
                  :string,total_amount :decimal(16, 2),subject :string,payment_status :string,create_time :string,callback_time
                  :string,callback_content :string> comment '数据',
    `old`  map<string,string> comment '旧值'
) comment '支付表'
    partitioned by (`dt` string)
    row format serde 'org.apache.hadoop.hive.serde2.JsonSerDe'
    location '/warehouse/gmall/ods/ods_payment_info_inc/';


-- 订单状态流水表
set hivevar:do_date = 2020-06-14;
load data inpath '/origin_data/gmall/db/payment_info_inc/${do_date}'
    into table ods_payment_info_inc
    partition (dt = '${do_date}');


-- 查询数据
select type,
       ts,
       data.id,
       data.out_trade_no,
       data.order_id,
       data.user_id,
       data.payment_type,
       data.trade_no,
       data.total_amount,
       data.subject,
       data.payment_status,
       data.create_time,
       data.callback_time,
       data.callback_content,
       old,
       dt
from ods_payment_info_inc
where dt = '${do_date}'
limit 10;