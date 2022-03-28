-- 创建 交易域用户粒度订单最近1日汇总表

drop table if exists dws_trade_user_order_1d;

create external table dws_trade_user_order_1d
(
    `user_id`                   STRING COMMENT '用户id',
    `order_count_1d`            BIGINT COMMENT '最近1日下单次数',
    `order_num_1d`              BIGINT COMMENT '最近1日下单商品件数',
    `order_original_amount_1d`  DECIMAL(16, 2) COMMENT '最近1日最近1日下单原始金额',
    `activity_reduce_amount_1d` DECIMAL(16, 2) COMMENT '最近1日下单活动优惠金额',
    `coupon_reduce_amount_1d`   DECIMAL(16, 2) COMMENT '下单优惠券优惠金额',
    `order_total_amount_1d`     DECIMAL(16, 2) COMMENT '最近1日下单最终金额'
) comment '交易域用户粒度订单最近1日汇总事实表'
    partitioned by (`dt` string)
    stored as orc
    location '/warehouse/gmall/dws/dws_trade_user_order_1d'
    tblproperties ('orc.compress' = 'snappy');


-- 首日装载 交易域用户粒度订单最近1日汇总表

insert overwrite table dws_trade_user_order_1d
    partition (dt)
select user_id,
       count(distinct (order_id)),
       sum(sku_num),
       sum(split_original_amount),
       sum(nvl(split_activity_amount, 0)),
       sum(nvl(split_coupon_amount, 0)),
       sum(split_total_amount),
       dt
from dwd_trade_order_detail_inc
group by user_id, dt;


-- 每日装载 交易域用户粒度订单最近1日汇总表

insert overwrite table dws_trade_user_order_1d
    partition (dt = '2020-06-15')
select user_id,
       count(distinct (order_id)),
       sum(sku_num),
       sum(split_original_amount),
       sum(nvl(split_activity_amount, 0)),
       sum(nvl(split_coupon_amount, 0)),
       sum(split_total_amount)
from dwd_trade_order_detail_inc
where dt = '2020-06-15'
group by user_id;


-- 查看分区

show partitions dws_trade_user_order_1d;


-- 查看 交易域用户粒度订单最近1日汇总表

select user_id,
       order_count_1d,
       order_num_1d,
       order_original_amount_1d,
       activity_reduce_amount_1d,
       coupon_reduce_amount_1d,
       order_total_amount_1d,
       dt
from dws_trade_user_order_1d
where dt = '2020-06-14';


