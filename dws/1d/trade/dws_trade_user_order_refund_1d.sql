-- 创建 交易域用户粒度退单最近1日汇总表

drop table if exists dws_trade_user_order_refund_1d;

create external table dws_trade_user_order_refund_1d
(
    `user_id`                STRING COMMENT '用户id',
    `order_refund_count_1d`  BIGINT COMMENT '最近1日退单次数',
    `order_refund_num_1d`    BIGINT COMMENT '最近1日退单商品件数',
    `order_refund_amount_1d` DECIMAL(16, 2) COMMENT '最近1日退单金额'
) comment '交易域用户粒度退单最近1日汇总事实表'
    partitioned by (`dt` string)
    stored as orc
    location '/warehouse/gmall/dws/dws_trade_user_order_refund_1d'
    tblproperties ('orc.compress' = 'snappy');


-- 首日装载 交易域用户粒度退单最近1日汇总表

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_trade_user_order_refund_1d
    partition (dt)
select user_id,
       count(*)           order_refund_count,
       sum(refund_num)    order_refund_num,
       sum(refund_amount) order_refund_amount,
       dt
from dwd_trade_order_refund_inc
group by user_id, dt;


-- 每日装载 交易域用户粒度退单最近1日汇总表

insert overwrite table dws_trade_user_order_refund_1d
    partition (dt = '2020-06-15')
select user_id,
       count(*),
       sum(refund_num),
       sum(refund_amount)
from dwd_trade_order_refund_inc
where dt = '2020-06-15'
group by user_id;


-- 查看分区

show partitions dws_trade_user_order_refund_1d;



-- 查看 交易域用户粒度退单最近1日汇总表

select user_id,
       order_refund_count_1d,
       order_refund_num_1d,
       order_refund_amount_1d,
       dt
from dws_trade_user_order_refund_1d
where dt = '2020-06-14';



