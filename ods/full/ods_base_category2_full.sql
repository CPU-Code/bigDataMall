-- 删除 二级品类表
drop table if exists ods_base_category2_full;

-- 创建 二级品类表
create external table ods_base_category2_full
(
    `id`           string comment '编号',
    `name`         string comment '二级分类名称',
    `category1_id` string comment '一级分类编号'
) comment '二级品类表'
    partitioned by (`dt` STRING)
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/warehouse/gmall/ods/ods_base_category2_full/';

-- 装载数据
load data inpath '/origin_data/gmall/db/base_category2_full/2020-06-14'
    into table ods_base_category2_full
    partition (dt = '2020-06-14');


-- 查询数据
select id,
       name,
       category1_id,
       dt
from ods_base_category2_full
where dt = '2020-06-14';
