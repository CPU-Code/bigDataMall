-- 创建 交易域用户粒度订单历史至今汇总表

drop table if exists dws_trade_user_order_td;

create external table dws_trade_user_order_td
(
    `user_id`                   STRING COMMENT '用户id',
    `order_date_first`          STRING COMMENT '首次下单日期',
    `order_date_last`           STRING COMMENT '末次下单日期',
    `order_count_td`            BIGINT COMMENT '下单次数',
    `order_num_td`              BIGINT COMMENT '购买商品件数',
    `original_amount_td`        DECIMAL(16, 2) COMMENT '原始金额',
    `activity_reduce_amount_td` DECIMAL(16, 2) COMMENT '活动优惠金额',
    `coupon_reduce_amount_td`   DECIMAL(16, 2) COMMENT '优惠券优惠金额',
    `total_amount_td`           DECIMAL(16, 2) COMMENT '最终金额'
) comment '交易域用户粒度订单历史至今汇总事实表'
    partitioned by (`dt` string)
    stored as orc
    location '/warehouse/gmall/dws/dws_trade_user_order_td'
    tblproperties ('orc.compress' = 'snappy');


-- 首日装载 交易域用户粒度订单历史至今汇总表

insert overwrite table dws_trade_user_order_td
    partition (dt = '2020-06-14')
select user_id,
       min(dt)                        login_date_first,
       max(dt)                        login_date_last,
       sum(order_count_1d)            order_count,
       sum(order_num_1d)              order_num,
       sum(order_original_amount_1d)  original_amount,
       sum(activity_reduce_amount_1d) activity_reduce_amount,
       sum(coupon_reduce_amount_1d)   coupon_reduce_amount,
       sum(order_total_amount_1d)     total_amount
from dws_trade_user_order_1d
group by user_id;


-- 每日装载 交易域用户粒度订单历史至今汇总表

insert overwrite table dws_trade_user_order_td
    partition (dt = '2020-06-15')
select nvl(old.user_id, new.user_id),
       if(new.user_id is not null and old.user_id is null, '2020-06-15', old.order_date_first),
       if(new.user_id is not null, '2020-06-15', old.order_date_last),
       nvl(old.order_count_td, 0) + nvl(new.order_count_1d, 0),
       nvl(old.order_num_td, 0) + nvl(new.order_num_1d, 0),
       nvl(old.original_amount_td, 0) + nvl(new.order_original_amount_1d, 0),
       nvl(old.activity_reduce_amount_td, 0) + nvl(new.activity_reduce_amount_1d, 0),
       nvl(old.coupon_reduce_amount_td, 0) + nvl(new.coupon_reduce_amount_1d, 0),
       nvl(old.total_amount_td, 0) + nvl(new.order_total_amount_1d, 0)
from (
         select user_id,
                order_date_first,
                order_date_last,
                order_count_td,
                order_num_td,
                original_amount_td,
                activity_reduce_amount_td,
                coupon_reduce_amount_td,
                total_amount_td
         from dws_trade_user_order_td
         where dt = date_add('2020-06-15', -1)
     ) old
         full outer join (
    select user_id,
           order_count_1d,
           order_num_1d,
           order_original_amount_1d,
           activity_reduce_amount_1d,
           coupon_reduce_amount_1d,
           order_total_amount_1d
    from dws_trade_user_order_1d
    where dt = '2020-06-15'
) new
                         on old.user_id = new.user_id;


-- 查看分区

show partitions dws_trade_user_order_td;


-- 查看 交易域用户粒度订单历史至今汇总表

select user_id,
       order_date_first,
       order_date_last,
       order_count_td,
       order_num_td,
       original_amount_td,
       activity_reduce_amount_td,
       coupon_reduce_amount_td,
       total_amount_td,
       dt
from dws_trade_user_order_td
where dt = '2020-06-14';






