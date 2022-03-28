-- 创建 交易域用户粒度退单最近n日汇总表

drop table if exists dws_trade_user_order_refund_nd;

create external table dws_trade_user_order_refund_nd
(
    `user_id`                 STRING COMMENT '用户id',
    `order_refund_count_7d`   BIGINT COMMENT '最近7日退单次数',
    `order_refund_num_7d`     BIGINT COMMENT '最近7日退单商品件数',
    `order_refund_amount_7d`  DECIMAL(16, 2) COMMENT '最近7日退单金额',
    `order_refund_count_30d`  BIGINT COMMENT '最近30日退单次数',
    `order_refund_num_30d`    BIGINT COMMENT '最近30日退单商品件数',
    `order_refund_amount_30d` DECIMAL(16, 2) COMMENT '最近30日退单金额'
) comment '交易域用户粒度退单最近n日汇总事实表'
    partitioned by (`dt` string)
    stored as orc
    location '/warehouse/gmall/dws/dws_trade_user_order_refund_nd'
    tblproperties ('orc.compress' = 'snappy');


-- 数据装载 交易域用户粒度退单最近n日汇总表

insert overwrite table dws_trade_user_order_refund_nd
    partition (dt = '2020-06-14')
select user_id,
       sum(if(dt >= date_add('2020-06-14', -6), order_refund_count_1d, 0)),
       sum(if(dt >= date_add('2020-06-14', -6), order_refund_num_1d, 0)),
       sum(if(dt >= date_add('2020-06-14', -6), order_refund_amount_1d, 0)),
       sum(order_refund_count_1d),
       sum(order_refund_num_1d),
       sum(order_refund_amount_1d)
from dws_trade_user_order_refund_1d
where dt >= date_add('2020-06-14', -29)
  and dt <= '2020-06-14'
group by user_id;


-- 查看分区

show partitions dws_trade_user_order_refund_nd;


-- 查看 交易域用户粒度退单最近n日汇总表

select user_id,
       order_refund_count_7d,
       order_refund_num_7d,
       order_refund_amount_7d,
       order_refund_count_30d,
       order_refund_num_30d,
       order_refund_amount_30d,
       dt
from dws_trade_user_order_refund_nd
where dt = '2020-06-14';
