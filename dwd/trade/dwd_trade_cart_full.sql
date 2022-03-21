-- 创建 购物车周期快照事实表

drop table if exists dwd_trade_cart_full;

create external table dwd_trade_cart_full
(
    `id`       STRING COMMENT '编号',
    `user_id`  STRING COMMENT '用户id',
    `sku_id`   STRING COMMENT '商品id',
    `sku_name` STRING COMMENT '商品名称',
    `sku_num`  BIGINT COMMENT '加购物车件数'
) comment '交易域购物车周期快照事实表'
    partitioned by (`dt` STRING)
    row format delimited fields terminated by '\t'
    stored as orc
    location '/warehouse/gmall/dwd/dwd_trade_cart_full/'
    tblproperties ('orc.compress' = 'snappy');


-- 首日装载 购物车周期快照事实表
insert overwrite table dwd_trade_cart_full
    partition (dt = '2020-06-14')
select id,
       user_id,
       sku_id,
       sku_name,
       sku_num
from ods_cart_info_full
where dt = '2020-06-14'
  and is_ordered = '0';


-- 查看分区

show partitions dwd_trade_cart_full;


-- 查看 购物车周期快照事实表

select id,
       user_id,
       sku_id,
       sku_name,
       sku_num,
       dt
from dwd_trade_cart_full
where dt = '2020-06-14';


