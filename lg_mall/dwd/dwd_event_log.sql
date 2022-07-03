-- 所有事件明细
drop table if exists dwd_event_log;

create external table dwd_event_log
(
    `device_id`   string comment '设备id',
    `uid`         string comment '用户id',
    `app_v`       string comment 'app版本',
    `os_type`     string comment '系统版本',
    `event_type`  string comment '事件类型',
    `language`    string comment '语言',
    `channel`     string comment '渠道',
    `area`        string comment '地区',
    `brand`       string comment '型号',
    `name`        string comment '数据',
    `event_json`  string comment '事件数据',
    `report_time` string comment '事件时间'
) comment '事件明细'
    partitioned by (`dt` string)
    stored as parquet;



-----------------------

-- 装载数据


-- 筛选 json
set hive.execution.engine = spark;
set hivevar:do_date= 2020-07-21;
select split(str, ' ')[7] as line
from ods_log_event
where dt = '${do_date}'
limit 10;


-- 解析 json 各项
set hivevar:do_date= 2020-07-21;
with tmp_start as (
    select split(str, ' ')[7] as line
    from ods_log_event
    where dt = '${do_date}'
)
select get_json_object(line, '$.attr.device_id')  as device_id,
       get_json_object(line, '$.attr.uid')        as uid,
       get_json_object(line, '$.attr.app_v')      as app_v,
       get_json_object(line, '$.attr.os_type')    as os_type,
       get_json_object(line, '$.attr.event_type') as event_type,
       get_json_object(line, '$.attr.language')   as language,
       get_json_object(line, '$.attr.channel')    as channel,
       get_json_object(line, '$.attr.area')       as area,
       get_json_object(line, '$.attr.brand')      as brand,
       get_json_object(line, '$.lagou_event')     as lagou_event
from tmp_start
limit 10;


-- 装载数据

add jar hdfs://powercluster/origin_data/cpucode/Hive-3.1.0-jar-with-dependencies.jar;
create temporary function json_array as 'com.cpucode.dw.hive.udf.ParseJsonArray';
set hive.execution.engine = spark;
set hivevar:do_date= 2020-07-21;
with tmp_start as (
    select split(str, ' ')[7] as line
    from ods_log_event
    where dt = '${do_date}'
)
insert
overwrite
table
dwd_event_log
partition
(
dt = '${do_date}'
)
select device_id,
       uid,
       app_v,
       os_type,
       event_type,
       language,
       channel,
       area,
       brand,
       get_json_object(k, '$.name') as name,
       get_json_object(k, '$.json') as event_json,
       get_json_object(k, '$.time') as report_time
from (
         select get_json_object(line, '$.attr.device_id')  as device_id,
                get_json_object(line, '$.attr.uid')        as uid,
                get_json_object(line, '$.attr.app_v')      as app_v,
                get_json_object(line, '$.attr.os_type')    as os_type,
                get_json_object(line, '$.attr.event_type') as event_type,
                get_json_object(line, '$.attr.language')   as language,
                get_json_object(line, '$.attr.channel')    as channel,
                get_json_object(line, '$.attr.area')       as area,
                get_json_object(line, '$.attr.brand')      as brand,
                get_json_object(line, '$.lagou_event')     as lagou_event
         from tmp_start
     ) A lateral view explode(json_array(lagou_event)) B as k;




--------------------------------


-- 查询数据
set hivevar:do_date= 2020-07-21;
select device_id,
       uid,
       app_v,
       os_type,
       event_type,
       language,
       channel,
       area,
       brand,
       name,
       event_json,
       report_time,
       dt
from dwd_event_log
where dt = '${do_date}'
limit 10;