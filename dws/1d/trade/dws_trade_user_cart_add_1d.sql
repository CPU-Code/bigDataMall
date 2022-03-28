-- 创建 交易域用户粒度加购最近1日汇总表

drop table if exists dws_trade_user_cart_add_1d;

create external table dws_trade_user_cart_add_1d
(
    `user_id`           STRING COMMENT '用户id',
    `cart_add_count_1d` BIGINT COMMENT '最近1日加购次数',
    `cart_add_num_1d`   BIGINT COMMENT '最近1日加购商品件数'
) comment '交易域用户粒度加购最近1日汇总事实表'
    partitioned by (`dt` string)
    stored as orc
    location '/warehouse/gmall/dws/dws_trade_user_cart_add_1d'
    tblproperties ('orc.compress' = 'snappy');


-- 首日装载 交易域用户粒度加购最近1日汇总表

insert overwrite table dws_trade_user_cart_add_1d
    partition (dt)
select user_id,
       count(*),
       sum(sku_num),
       dt
from dwd_trade_cart_add_inc
group by user_id, dt;


-- 每日装载 交易域用户粒度加购最近1日汇总表

insert overwrite table dws_trade_user_cart_add_1d
    partition (dt = '2020-06-15')
select user_id,
       count(*),
       sum(sku_num)
from dwd_trade_cart_add_inc
where dt = '2020-06-15'
group by user_id;


-- 查看分区

show partitions dws_trade_user_cart_add_1d;


-- 查看 交易域用户粒度加购最近1日汇总表

select user_id,
       cart_add_count_1d,
       cart_add_num_1d,
       dt
from dws_trade_user_cart_add_1d
where dt = '2020-06-15';
