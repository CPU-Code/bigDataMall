-- 删除 地区表
drop table if exists ods_base_region_full;

-- 创建 地区表
create external table ods_base_region_full
(
    `id`          string comment '编号',
    `region_name` string comment '地区名称'
) comment ''
    partitioned by (`dt` string)
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/warehouse/gmall/ods/ods_base_region_full/';


-- 装载数据
load data inpath '/origin_data/gmall/db/base_region_full/2020-06-14'
    into table ods_base_region_full
    partition (dt = '2020-06-14');


-- 查询数据
select id,
       region_name,
       dt
from ods_base_region_full
where dt = '2020-06-14';