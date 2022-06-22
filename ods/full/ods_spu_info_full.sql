-- 删除 SPU表
drop table if exists ods_spu_info_full;

-- 创建 SPU表
create external table ods_spu_info_full
(
    `id`           STRING COMMENT 'spu_id',
    `spu_name`     STRING COMMENT 'spu名称',
    `description`  STRING COMMENT '描述信息',
    `category3_id` STRING COMMENT '品类id',
    `tm_id`        STRING COMMENT '品牌id'
) comment 'SPU商品表'
    partitioned by (`dt` string)
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/warehouse/gmall/ods/ods_spu_info_full/';


-- 加载数据
load data inpath '/origin_data/gmall/db/spu_info_full/2020-06-14'
    into table ods_spu_info_full
    partition (dt = '2020-06-14');


-- 查询
select id,
       spu_name,
       description,
       category3_id,
       tm_id,
       dt
from ods_spu_info_full
where dt = '2020-06-14';
