-- 删除 三级品类表

drop table if exists ods_base_category3_full;

-- 创建外部 三级品类表

create external table ods_base_category3_full
(
    `id`           string comment '编号',
    `name`         string comment '三级分类名称',
    `category2_id` string comment '二级分类编号'
) comment '三级品类表'
    partitioned by (`dt` string)
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/warehouse/gmall/ods/ods_base_category3_full/';


-- 装载数据 三级品类表

load data inpath '/origin_data/gmall/db/base_category3_full/2020-06-14'
    into table ods_base_category3_full
    partition (dt = '2020-06-14');

-- 查询数据 三级品类表

select id,
       name,
       category2_id,
       dt
from ods_base_category3_full
where dt = '2020-06-14';