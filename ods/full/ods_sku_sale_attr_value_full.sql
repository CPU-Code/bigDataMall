-- 删除 sku销售属性名称
drop table if exists ods_sku_sale_attr_value_full;

-- 创建 sku销售属性名称
create external table ods_sku_sale_attr_value_full
(
    `id`                   string comment '编号',
    `sku_id`               string comment 'sku_id',
    `spu_id`               string comment 'spu_id',
    `sale_attr_value_id`   string comment '销售属性值id',
    `sale_attr_id`         string comment '销售属性id',
    `sale_attr_name`       string comment '销售属性名称',
    `sale_attr_value_name` string comment '销售属性值名称'
) comment 'sku销售属性名称'
    partitioned by (`dt` string)
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/warehouse/gmall/ods/ods_sku_sale_attr_value_full/';


-- 装载数据
load data inpath '/origin_data/gmall/db/sku_sale_attr_value_full/2020-06-14'
    into table ods_sku_sale_attr_value_full
    partition (dt = '2020-06-14');


-- 查询数据
select id,
       sku_id,
       spu_id,
       sale_attr_value_id,
       sale_attr_id,
       sale_attr_name,
       sale_attr_value_name,
       dt
from ods_sku_sale_attr_value_full
where dt = '2020-06-14';


