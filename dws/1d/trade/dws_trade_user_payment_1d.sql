-- 创建 交易域用户粒度支付最近1日汇总表

drop table if exists dws_trade_user_payment_1d;

create external table dws_trade_user_payment_1d
(
    `user_id`           STRING COMMENT '用户id',
    `payment_count_1d`  BIGINT COMMENT '最近1日支付次数',
    `payment_num_1d`    BIGINT COMMENT '最近1日支付商品件数',
    `payment_amount_1d` DECIMAL(16, 2) COMMENT '最近1日支付金额'
) COMMENT '交易域用户粒度支付最近1日汇总事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_trade_user_payment_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');


-- 首日装载 交易域用户粒度支付最近1日汇总表

insert overwrite table dws_trade_user_payment_1d
    partition (dt)
select user_id,
       count(distinct (order_id)),
       sum(sku_num),
       sum(split_payment_amount),
       dt
from dwd_trade_pay_detail_suc_inc
group by user_id, dt;


-- 每日装载 交易域用户粒度支付最近1日汇总表

insert overwrite table dws_trade_user_payment_1d
    partition (dt = '2020-06-15')
select user_id,
       count(distinct (order_id)),
       sum(sku_num),
       sum(split_payment_amount)
from dwd_trade_pay_detail_suc_inc
where dt = '2020-06-15'
group by user_id;


-- 查看分区

show partitions dws_trade_user_payment_1d;


-- 查看 交易域用户粒度支付最近1日汇总表

select user_id,
       payment_count_1d,
       payment_num_1d,
       payment_amount_1d,
       dt
from dws_trade_user_payment_1d
where dt = '2020-06-15';
