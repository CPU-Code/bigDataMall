-- 删除 活动信息表

DROP TABLE IF EXISTS ods_activity_info_full;

-- 创建外部 活动信息表

CREATE EXTERNAL TABLE ods_activity_info_full
(
    `id`            STRING COMMENT '活动id',
    `activity_name` STRING COMMENT '活动名称',
    `activity_type` STRING COMMENT '活动类型',
    `activity_desc` STRING COMMENT '活动描述',
    `start_time`    STRING COMMENT '开始时间',
    `end_time`      STRING COMMENT '结束时间',
    `create_time`   STRING COMMENT '创建时间'
) COMMENT '活动信息表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/gmall/ods/ods_activity_info_full/';


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

