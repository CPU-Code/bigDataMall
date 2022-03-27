-- 创建 交易域用户商品粒度退单最近1日汇总表

drop table if exists dws_trade_user_order_refund_1d;

create external table dws_trade_user_sku_order_refund_1d
(
    `user_id`                STRING COMMENT '用户id',
    `sku_id`                 STRING COMMENT 'sku_id',
    `sku_name`               STRING COMMENT 'sku名称',
    `category1_id`           STRING COMMENT '一级分类id',
    `category1_name`         STRING COMMENT '一级分类名称',
    `category2_id`           STRING COMMENT '一级分类id',
    `category2_name`         STRING COMMENT '一级分类名称',
    `category3_id`           STRING COMMENT '一级分类id',
    `category3_name`         STRING COMMENT '一级分类名称',
    `tm_id`                  STRING COMMENT '品牌id',
    `tm_name`                STRING COMMENT '品牌名称',
    `order_refund_count_1d`  BIGINT COMMENT '最近1日退单次数',
    `order_refund_num_1d`    BIGINT COMMENT '最近1日退单件数',
    `order_refund_amount_1d` DECIMAL(16, 2) COMMENT '最近1日退单金额'
) comment '交易域用户商品粒度退单最近1日汇总事实表'
    partitioned by (`dt` string)
    stored as orc
    location '/warehouse/gmall/dws/dws_trade_user_sku_order_refund_1d'
    tblproperties ('orc.compress' = 'snappy');


-- 首日装载 交易域用户商品粒度退单最近 1 日汇总表

insert overwrite table dws_trade_user_sku_order_refund_1d
    partition (dt)
select user_id,
       sku_id,
       sku_name,
       category1_id,
       category1_name,
       category2_id,
       category2_name,
       category3_id,
       category3_name,
       tm_id,
       tm_name,
       order_refund_count,
       order_refund_num,
       order_refund_amount,
       dt
from (
         select dt,
                user_id,
                sku_id,
                count(*)           order_refund_count,
                sum(refund_num)    order_refund_num,
                sum(refund_amount) order_refund_amount
         from dwd_trade_order_refund_inc
         group by dt, user_id, sku_id
     ) od
         left join(
    select id,
           sku_name,
           category1_id,
           category1_name,
           category2_id,
           category2_name,
           category3_id,
           category3_name,
           tm_id,
           tm_name
    from dim_sku_full
    where dt = '2020-06-14'
) sku
                  on od.sku_id = sku.id;


-- 每日装载 交易域用户商品粒度退单最近1日汇总表

insert overwrite table dws_trade_user_sku_order_refund_1d
    partition (dt = '2020-06-15')
select user_id,
       sku_id,
       sku_name,
       category1_id,
       category1_name,
       category2_id,
       category2_name,
       category3_id,
       category3_name,
       tm_id,
       tm_name,
       order_refund_count,
       order_refund_num,
       order_refund_amount
from (
         select user_id,
                sku_id,
                count(*)           order_refund_count,
                sum(refund_num)    order_refund_num,
                sum(refund_amount) order_refund_amount
         from dwd_trade_order_refund_inc
         where dt = '2020-06-15'
         group by user_id, sku_id
     ) od
         left join (
    select id,
           sku_name,
           category1_id,
           category1_name,
           category2_id,
           category2_name,
           category3_id,
           category3_name,
           tm_id,
           tm_name
    from dim_sku_full
    where dt = '2020-06-15'
) sku
                   on od.sku_id = sku.id;


-- 查看分区

show partitions dws_trade_user_sku_order_refund_1d;


-- 查看 交易域用户商品粒度退单最近1日汇总表

select user_id,
       sku_id,
       sku_name,
       category1_id,
       category1_name,
       category2_id,
       category2_name,
       category3_id,
       category3_name,
       tm_id,
       tm_name,
       order_refund_count_1d,
       order_refund_num_1d,
       order_refund_amount_1d,
       dt
from dws_trade_user_sku_order_refund_1d
where dt = '2020-06-14'

