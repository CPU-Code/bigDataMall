-- 删除 商品表
drop table if exists ods_sku_info_full;

-- 创建 商品表
create external table ods_sku_info_full
(
    `id`              string comment 'skuId',
    `spu_id`          string comment 'spuid',
    `price`           decimal(16, 2) comment '价格',
    `sku_name`        string comment '商品名称',
    `sku_desc`        string comment '商品描述',
    `weight`          decimal(16, 2) comment '重量',
    `tm_id`           string comment '品牌id',
    `category3_id`    string comment '品类id',
    `sku_default_igm` string comment '商品图片地址',
    `is_sale`         string comment '是否在售',
    `create_time`     string comment '创建时间'
) comment 'SKU商品表'
    partitioned by (`dt` string)
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/warehouse/gmall/ods/ods_sku_info_full/';

-- 装载数据
load data inpath '/origin_data/gmall/db/sku_info_full/2020-06-14'
    into table ods_sku_info_full
    partition (dt = '2020-06-14');


-- 查询数据
select id,
       spu_id,
       price,
       sku_name,
       sku_desc,
       weight,
       tm_id,
       category3_id,
       sku_default_igm,
       is_sale,
       create_time,
       dt
from ods_sku_info_full
where dt = '2020-06-14';