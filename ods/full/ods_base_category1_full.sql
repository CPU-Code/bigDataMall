-- 删除 一级品类表

drop table if exists ods_base_category1_full;

-- 创建 一级品类表

create external table ods_base_category1_full
(
    `id`   string comment '编号',
    `name` string comment '分类名称'
) comment '一级品类表'
    partitioned by (`dt` string)
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/warehouse/gmall/ods/ods_base_category1_full/';


-- 装载数据
load data inpath '/origin_data/gmall/db/base_category1_full/2020-06-14'
    into table ods_base_category1_full
    partition (dt = '2020-06-14');

-- 查询数据

select id,
       name,
       dt
from ods_base_category1_full
where dt = '2020-06-14';




