-- 创建 交易域用户粒度支付最近n日汇总表

drop table if exists dws_trade_user_payment_nd;

create external table dws_trade_user_payment_nd
(
    `user_id`            STRING COMMENT '用户id',
    `payment_count_7d`   BIGINT COMMENT '最近7日支付次数',
    `payment_num_7d`     BIGINT COMMENT '最近7日支付商品件数',
    `payment_amount_7d`  DECIMAL(16, 2) COMMENT '最近7日支付金额',
    `payment_count_30d`  BIGINT COMMENT '最近30日支付次数',
    `payment_num_30d`    BIGINT COMMENT '最近30日支付商品件数',
    `payment_amount_30d` DECIMAL(16, 2) COMMENT '最近30日支付金额'
) comment '交易域用户粒度支付最近n日汇总事实表'
    partitioned by (`dt` string)
    stored as orc
    location '/warehouse/gmall/dws/dws_trade_user_payment_nd'
    tblproperties ('orc.compress' = 'snappy');


-- 每日装载 交易域用户粒度支付最近n日汇总表

insert overwrite table dws_trade_user_payment_nd
    partition (dt = '2020-06-14')
select user_id,
       sum(if(dt >= date_add('2020-06-14', -6), payment_count_1d, 0)),
       sum(if(dt >= date_add('2020-06-14', -6), payment_num_1d, 0)),
       sum(if(dt >= date_add('2020-06-14', -6), payment_amount_1d, 0)),
       sum(payment_count_1d),
       sum(payment_num_1d),
       sum(payment_amount_1d)
from dws_trade_user_payment_1d
where dt >= date_add('2020-06-14', -29)
  and dt <= '2020-06-14'
group by user_id;


-- 查看分区

show partitions dws_trade_user_payment_nd;


-- 查看 交易域用户粒度支付最近n日汇总表

select user_id,
       payment_count_7d,
       payment_num_7d,
       payment_amount_7d,
       payment_count_30d,
       payment_num_30d,
       payment_amount_30d,
       dt
from dws_trade_user_payment_nd
where dt = '2020-06-14';

