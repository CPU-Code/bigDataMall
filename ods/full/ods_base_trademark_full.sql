-- 删除 品牌表
drop table if exists ods_base_trademark_full;

-- 创建 品牌表
create external table ods_base_trademark_full
(
    `id`       string comment '编号',
    `tm_name`  string comment '品牌名称',
    `logo_url` string comment '品牌logo的图片路径'
) comment '品牌表'
    partitioned by (`dt` string)
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/warehouse/gmall/ods/ods_base_trademark_full/';


-- 装载数据
load data inpath '/origin_data/gmall/db/base_trademark_full/2020-06-14'
    into table ods_base_trademark_full
    partition (dt = '2020-06-14');


-- 查询数据
select id,
       tm_name,
       logo_url,
       dt
from ods_base_trademark_full
where dt = '2020-06-14';




