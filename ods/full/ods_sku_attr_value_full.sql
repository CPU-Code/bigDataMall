-- 删除 商品平台属性表
drop table if exists ods_sku_attr_value_full;

-- 创建 商品平台属性表
create external table ods_sku_attr_value_full
(
    `id`          string comment '编号',
    `attr_id`     string comment '平台属性ID',
    `value_id`    string comment '平台属性值ID',
    `sku_id`      string comment '商品ID',
    `attr_name`   string comment '平台属性名称',
    `values_name` string comment '平台属性值名称'
) comment 'sku平台属性表'
    partitioned by (`dt` string)
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/warehouse/gmall/ods/ods_sku_attr_value_full/';


-- 装载数据
load data inpath '/origin_data/gmall/db/sku_attr_value_full/2020-06-14'
    into table ods_sku_attr_value_full
    partition (dt = '2020-06-14');


-- 查询数据
select id,
       attr_id,
       value_id,
       sku_id,
       attr_name,
       values_name,
       dt
from ods_sku_attr_value_full
where dt = '2020-06-14';