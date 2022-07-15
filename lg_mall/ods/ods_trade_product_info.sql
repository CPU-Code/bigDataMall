-- 产品信息表
drop table if exists `ods_trade_product_info`;

create external table ods_trade_product_info
(
    `product_id`   bigint comment '商品 id',
    `product_name` string comment '商品名称',
    `shop_id`      string comment '门店 ID',
    `price`        decimal(10, 0) comment '门店价',
    `issale`       tinyint comment '是否上架 0:不上架 1:上架',
    `status`       tinyint comment '是否新品 0:否 1:是',
    `category_id`  string comment 'goodsCatId 最后一级商品分类ID',
    `create_time`  string comment '创建时间',
    `modify_time`  string comment '更新时间'
) comment '产品信息表'
    partitioned by (`dt` string)
    row format delimited fields terminated by ','
    location '/origin_data/lg_mall/trade.db/product_info/';


-- 装载数据
set hivevar:do_date = 2020-07-12;
alter table ods_trade_product_info
    add partition (dt = '${do_date}');


-- 查询数据
set hivevar:do_date = 2020-07-12;
select product_id,
       product_name,
       shop_id,
       price,
       issale,
       status,
       category_id,
       create_time,
       modify_time,
       dt
from ods_trade_product_info
where dt = '${do_date}'
limit 10;