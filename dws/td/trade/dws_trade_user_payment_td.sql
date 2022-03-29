-- 创建 交易域用户粒度支付历史至今汇总表

drop table if exists dws_trade_user_payment_td;

create external table dws_trade_user_payment_td
(
    `user_id`            STRING COMMENT '用户id',
    `payment_date_first` STRING COMMENT '首次支付日期',
    `payment_date_last`  STRING COMMENT '末次支付日期',
    `payment_count_td`   BIGINT COMMENT '最近7日支付次数',
    `payment_num_td`     BIGINT COMMENT '最近7日支付商品件数',
    `payment_amount_td`  DECIMAL(16, 2) COMMENT '最近7日支付金额'
) comment '交易域用户粒度支付历史至今汇总事实表'
    partitioned by (`dt` string)
    stored as orc
    location '/warehouse/gmall/dws/dws_trade_user_payment_td'
    tblproperties ('orc.compress' = 'snappy');


-- 首日装载 交易域用户粒度支付历史至今汇总表

insert overwrite table dws_trade_user_payment_td
    partition (dt = '2020-06-14')
select user_id,
       min(dt)                payment_date_first,
       max(dt)                payment_date_last,
       sum(payment_count_1d)  payment_count,
       sum(payment_num_1d)    payment_num,
       sum(payment_amount_1d) payment_amount
from dws_trade_user_payment_1d
group by user_id;


-- 每日装载 交易域用户粒度支付历史至今汇总表

insert overwrite table dws_trade_user_payment_td
    partition (dt = '2020-06-15')
select nvl(old.user_id, new.user_id),
       if(old.user_id is null and new.user_id is not null, '2020-06-15', old.payment_date_first),
       if(new.user_id is not null, '2020-06-15', old.payment_date_last),
       nvl(old.payment_count_td, 0) + nvl(new.payment_count_1d, 0),
       nvl(old.payment_num_td, 0) + nvl(new.payment_num_1d, 0),
       nvl(old.payment_amount_td, 0) + nvl(new.payment_amount_1d, 0)
from (
         select user_id,
                payment_date_first,
                payment_date_last,
                payment_count_td,
                payment_num_td,
                payment_amount_td
         from dws_trade_user_payment_td
         where dt = date_add('2020-06-15', -1)
     ) old
         full outer join (
    select user_id,
           payment_count_1d,
           payment_num_1d,
           payment_amount_1d
    from dws_trade_user_payment_1d
    where dt = '2020-06-15'
) new
                         on old.user_id = new.user_id;


-- 查看分区

show partitions dws_trade_user_payment_td;


-- 查看 交易域用户粒度支付历史至今汇总表

select user_id,
       payment_date_first,
       payment_date_last,
       payment_count_td,
       payment_num_td,
       payment_amount_td,
       dt
from dws_trade_user_payment_td
where dt = '2020-06-14';







