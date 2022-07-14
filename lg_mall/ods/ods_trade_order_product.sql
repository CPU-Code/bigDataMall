-- 订单明细表
drop table if exists `ods_trade_order_product`;

create external table ods_trade_order_product
(
    `id`            string comment 'id',
    `order_id`      decimal(10, 2) comment '订单id',
    `product_id`    string comment '用户id',
    `product_num`   string comment '商品数量',
    `product_price` string comment '商品价格',
    `money`         string comment '付款金额',
    `extra`         string comment '额外信息',
    `create_time`   string comment '创建时间'
) comment '订单明细表'
    partitioned by (`dt` string)
    row format delimited fields terminated by ','
    location '/origin_data/lg_mall/trade.db/order_product/';

--------------

-- 装载数据
set hivevar:do_date = 2020-07-12;
alter table ods_trade_order_product
    add partition (dt = '${do_date}');


--------

-- 查询
set hive.execution.engine = spark;
set hivevar:do_date = 2020-07-12;
select id,
       order_id,
       product_id,
       product_num,
       product_price,
       money,
       extra,
       create_time,
       dt
from ods_trade_order_product
where dt = '${do_date}';