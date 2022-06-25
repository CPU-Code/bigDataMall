-- 删除启动日志明细表
drop table if exists dwd_start_log;

-- 创建启动日志明细表
create table dwd_start_log
(
    `device_id`  string comment '设备id',
    `area`       string comment '位置',
    `uid`        string comment '用户id',
    `app_v`      string,
    `event_type` string,
    `os_type`    string comment '系统版本',
    `channel`    string comment '渠道',
    `language`   string comment '语言',
    `brand`      string comment '型号',
    `entry`      string,
    `action`     string,
    `error_code` string comment '错误代码'
) comment '启动日志明细表'
    partitioned by (`dt` string)
    stored as parquet;


-- 装载数据
set hive.execution.engine = spark;
with tmp as (
    select split(str, ' ')[7] line
    from ods_start_log
    where dt = '2020-07-21'
)
insert
overwrite
table
dwd_start_log
partition
(
dt = '2020-07-21'
)
select get_json_object(line, '$.attr.device_id'),
       get_json_object(line, '$.attr.area'),
       get_json_object(line, '$.attr.uid'),
       get_json_object(line, '$.attr.app_v'),
       get_json_object(line, '$.attr.event_type'),
       get_json_object(line, '$.attr.os_type'),
       get_json_object(line, '$.attr.channel'),
       get_json_object(line, '$.attr.language'),
       get_json_object(line, '$.attr.brand'),
       get_json_object(line, '$.app_active.json.entry'),
       get_json_object(line, '$.app_active.json.action'),
       get_json_object(line, '$.app_active.json.error_code')
from tmp;


-- 查询数据
select device_id,
       area,
       uid,
       app_v,
       event_type,
       os_type,
       channel,
       `language`,
       brand,
       entry,
       `action`,
       error_code,
       dt
from dwd_start_log
where dt = '2020-07-21'
limit 100;