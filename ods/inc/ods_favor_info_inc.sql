-- 删除 收藏表
drop table if exists ods_favor_info_inc;

-- 创建 收藏表
create external table ods_favor_info_inc
(
    `type` string comment '变动类型',
    `ts`   bigint comment '变动时间',
    `data` struct<id :string, user_id :string, sku_id :string, spu_id :string, is_cancel :string, create_time :string,
                  cancel_time :string> comment '数据',
    `old`  map<string,string> comment '旧值'
) comment '收藏表'
    partitioned by (`dt` string)
    row format serde 'org.apache.hadoop.hive.serde2.JsonSerDe'
    location '/warehouse/gmall/ods/ods_favor_info_inc/';

-- 装载数据
load data inpath '/origin_data/gmall/db/favor_info_inc/2020-06-14'
    into table ods_favor_info_inc
    partition (dt = '2020-06-14');

-- 查询数据
select type,
       ts,
       data,
       old,
       dt
from ods_favor_info_inc
where dt = '2020-06-14';