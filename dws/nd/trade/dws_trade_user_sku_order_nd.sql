-- 创建 交易域用户商品粒度订单最近n日汇总表

drop table if exists dws_trade_user_sku_order_nd;

create external table dws_trade_user_sku_order_nd
(
    `user_id`                    STRING COMMENT '用户id',
    `sku_id`                     STRING COMMENT 'sku_id',
    `sku_name`                   STRING COMMENT 'sku名称',
    `category1_id`               STRING COMMENT '一级分类id',
    `category1_name`             STRING COMMENT '一级分类名称',
    `category2_id`               STRING COMMENT '一级分类id',
    `category2_name`             STRING COMMENT '一级分类名称',
    `category3_id`               STRING COMMENT '一级分类id',
    `category3_name`             STRING COMMENT '一级分类名称',
    `tm_id`                      STRING COMMENT '品牌id',
    `tm_name`                    STRING COMMENT '品牌名称',
    `order_count_7d`             STRING COMMENT '最近7日下单次数',
    `order_num_7d`               BIGINT COMMENT '最近7日下单件数',
    `order_original_amount_7d`   DECIMAL(16, 2) COMMENT '最近7日下单原始金额',
    `activity_reduce_amount_7d`  DECIMAL(16, 2) COMMENT '最近7日活动优惠金额',
    `coupon_reduce_amount_7d`    DECIMAL(16, 2) COMMENT '最近7日优惠券优惠金额',
    `order_total_amount_7d`      DECIMAL(16, 2) COMMENT '最近7日下单最终金额',
    `order_count_30d`            BIGINT COMMENT '最近30日下单次数',
    `order_num_30d`              BIGINT COMMENT '最近30日下单件数',
    `order_original_amount_30d`  DECIMAL(16, 2) COMMENT '最近30日下单原始金额',
    `activity_reduce_amount_30d` DECIMAL(16, 2) COMMENT '最近30日活动优惠金额',
    `coupon_reduce_amount_30d`   DECIMAL(16, 2) COMMENT '最近30日优惠券优惠金额',
    `order_total_amount_30d`     DECIMAL(16, 2) COMMENT '最近30日下单最终金额'
) comment '交易域用户商品粒度订单最近n日汇总事实表'
    partitioned by (`dt` string)
    stored as orc
    location '/warehouse/gmall/dws/dws_trade_user_sku_order_nd'
    tblproperties ('orc.compress' = 'snappy');


-- 数据装载 交易域用户商品粒度订单最近n日汇总表

insert overwrite table dws_trade_user_sku_order_nd
    partition (dt = '2020-06-14')
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
from dws_trade_user_sku_order_1d
where dt >= date_add('2020-06-14', -29)
group by user_id, sku_id, sku_name, category1_id, category1_name, category2_id, category2_name, category3_id,
         category3_name, tm_id, tm_name;


-- 查看分区

show partitions dws_trade_user_sku_order_nd;


-- 查看 交易域用户商品粒度订单最近n日汇总表

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
from dws_trade_user_sku_order_nd
where dt = '2020-06-14'

