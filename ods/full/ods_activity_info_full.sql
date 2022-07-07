-- 删除 活动信息表
drop table if exists ods_activity_info_full;

-- 创建外部 活动信息表

create external table ods_activity_info_full
(
    `id`            string comment '活动id',
    `activity_name` string comment '活动名称',
    `activity_type` string comment '活动类型',
    `activity_desc` string comment '活动描述',
    `start_time`    string comment '开始时间',
    `end_time`      string comment '结束时间',
    `create_time`   string comment '创建时间'
) comment '活动信息表'
    partitioned by (`dt` string)
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/warehouse/gmall/ods/ods_activity_info_full/';


-- 装载数据

load data inpath '/origin_data/gmall/db/activity_info_full/2020-06-14'
    into table ods_activity_info_full
    partition (dt = '2020-06-14');


-- 查询 活动信息表

select id,
       activity_name,
       activity_type,
       activity_desc,
       start_time,
       end_time,
       create_time,
       dt
from ods_activity_info_full
where dt = '2020-06-14'

