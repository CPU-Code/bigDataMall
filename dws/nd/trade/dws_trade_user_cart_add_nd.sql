-- 创建 交易域用户粒度加购最近n日汇总表

drop table if exists dws_trade_user_cart_add_nd;

create external table dws_trade_user_cart_add_nd
(
    `user_id`            STRING COMMENT '用户id',
    `cart_add_count_7d`  BIGINT COMMENT '最近7日加购次数',
    `cart_add_num_7d`    BIGINT COMMENT '最近7日加购商品件数',
    `cart_add_count_30d` BIGINT COMMENT '最近30日加购次数',
    `cart_add_num_30d`   BIGINT COMMENT '最近30日加购商品件数'
) comment '交易域用户粒度加购最近n日汇总事实表'
    partitioned by (`dt` string)
    stored as orc
    location '/warehouse/gmall/dws/dws_trade_user_cart_add_nd'
    tblproperties ('orc.compress' = 'snappy');


-- 数据装载 交易域用户粒度加购最近n日汇总表

insert overwrite table dws_trade_user_cart_add_nd
    partition (dt = '2020-06-14')
select user_id,
       sum(if(dt >= date_add('2020-06-14', -6), cart_add_count_1d, 0)),
       sum(if(dt >= date_add('2020-06-14', -6), cart_add_num_1d, 0)),
       sum(cart_add_count_1d),
       sum(cart_add_num_1d)
from dws_trade_user_cart_add_1d
where dt >= date_add('2020-06-14', -29)
  and dt <= '2020-06-14'
group by user_id;


-- 查看分区

show partitions dws_trade_user_cart_add_nd;


-- 查看 交易域用户粒度加购最近n日汇总表

select user_id,
       cart_add_count_7d,
       cart_add_num_7d,
       cart_add_count_30d,
       cart_add_num_30d,
       dt
from dws_trade_user_cart_add_nd
where dt = '2020-06-14';