-- 创建订单状态流水表
drop table if exists ods_order_status_log_inc;

create external table ods_order_status_log_inc
(
    `type` string comment '变动类型',
    `ts`   bigint comment '变动时间',
    `data` struct<id :string,order_id :string,order_status :string,operate_time :string> comment '数据',
    `old`  map<string,string> comment '旧值'
) comment '订单状态流水表'
    partitioned by (`dt` string)
    row format serde 'org.apache.hadoop.hive.serde2.JsonSerDe'
    location '/warehouse/gmall/ods/ods_order_status_log_inc/';


-- 订单状态流水表
set hivevar:do_date = 2020-06-14;
load data inpath '/origin_data/gmall/db/order_status_log_inc/${do_date}'
    into table ods_order_status_log_inc
    partition (dt = '${do_date}');


-- 查询数据
select type,
       ts,
       data.id,
       data.order_id,
       data.order_status,
       data.operate_time,
       old,
       dt
from ods_order_status_log_inc
where dt = '${do_date}'
limit 10;