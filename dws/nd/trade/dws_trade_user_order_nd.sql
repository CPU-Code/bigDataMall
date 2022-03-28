-- 创建 交易域用户粒度订单最近n日汇总表

drop table if exists dws_trade_user_order_nd;

create external table dws_trade_user_order_nd
(
    `user_id`                    STRING COMMENT '用户id',
    `order_count_7d`             BIGINT COMMENT '最近7日下单次数',
    `order_num_7d`               BIGINT COMMENT '最近7日下单商品件数',
    `order_original_amount_7d`   DECIMAL(16, 2) COMMENT '最近7日下单原始金额',
    `activity_reduce_amount_7d`  DECIMAL(16, 2) COMMENT '最近7日下单活动优惠金额',
    `coupon_reduce_amount_7d`    DECIMAL(16, 2) COMMENT '最近7日下单优惠券优惠金额',
    `order_total_amount_7d`      DECIMAL(16, 2) COMMENT '最近7日下单最终金额',
    `order_count_30d`            BIGINT COMMENT '最近30日下单次数',
    `order_num_30d`              BIGINT COMMENT '最近30日下单商品件数',
    `order_original_amount_30d`  DECIMAL(16, 2) COMMENT '最近30日下单原始金额',
    `activity_reduce_amount_30d` DECIMAL(16, 2) COMMENT '最近30日下单活动优惠金额',
    `coupon_reduce_amount_30d`   DECIMAL(16, 2) COMMENT '最近30日下单优惠券优惠金额',
    `order_total_amount_30d`     DECIMAL(16, 2) COMMENT '最近30日下单最终金额'
) comment '交易域用户粒度订单最近n日汇总事实表'
    partitioned by (`dt` string)
    stored as orc
    location '/warehouse/gmall/dws/dws_trade_user_order_nd'
    tblproperties ('orc.compress' = 'snappy');


-- 数据装载 交易域用户粒度订单最近n日汇总表

insert overwrite table dws_trade_user_order_nd
    partition (dt = '2020-06-14')
select user_id,
       sum(if(dt >= date_add('2020-06-14', -6), order_count_1d, 0)),
       sum(if(dt >= date_add('2020-06-14', -6), order_num_1d, 0)),
       sum(if(dt >= date_add('2020-06-14', -6), order_original_amount_1d, 0)),
       sum(if(dt >= date_add('2020-06-14', -6), activity_reduce_amount_1d, 0)),
       sum(if(dt >= date_add('2020-06-14', -6), coupon_reduce_amount_1d, 0)),
       sum(if(dt >= date_add('2020-06-14', -6), order_total_amount_1d, 0)),
       sum(order_count_1d),
       sum(order_num_1d),
       sum(order_original_amount_1d),
       sum(activity_reduce_amount_1d),
       sum(coupon_reduce_amount_1d),
       sum(order_total_amount_1d)
from dws_trade_user_order_1d
where dt >= date_add('2020-06-14', -29)
  and dt <= '2020-06-14'
group by user_id;


-- 查看分区

show partitions dws_trade_user_order_nd;


-- 查看 交易域用户粒度订单最近n日汇总表

select user_id,
       order_count_7d,
       order_num_7d,
       order_original_amount_7d,
       activity_reduce_amount_7d,
       coupon_reduce_amount_7d,
       order_total_amount_7d,
       order_count_30d,
       order_num_30d,
       order_original_amount_30d,
       activity_reduce_amount_30d,
       coupon_reduce_amount_30d,
       order_total_amount_30d,
       dt
from dws_trade_user_order_nd
where dt = '2020-06-14';
