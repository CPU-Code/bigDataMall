-- 删除 省份表
drop table if exists ods_base_province_full;

-- 创建 省份表
create external table ods_base_province_full
(
    `id`         string comment '编号',
    `name`       string comment '省份名称',
    `region_id`  string comment '地区ID',
    `area_code`  string comment '地区编码',
    `iso_code`   string comment '旧版ISO-3166-2编码，供可视化使用',
    `iso_3166_2` string comment '新版IOS-3166-2编码，供可视化使用'
) comment '省份表'
    partitioned by (`dt` string)
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/warehouse/gmall/ods/ods_base_province_full/';


-- 装载数据
load data inpath '/origin_data/gmall/db/base_province_full/2020-06-14'
    into table ods_base_province_full
    partition (dt = '2020-06-14');


-- 查询数据
select id,
       name,
       region_id,
       area_code,
       iso_code,
       iso_3166_2,
       dt
from ods_base_province_full
where dt = '2020-06-14';







