-- 事件日志
drop table if exists ods_log_event;

create external table ods_log_event
(
    `str` string
) comment '事件日志'
    partitioned by (`dt` string)
    stored as textfile
    location '/user/yx_test/data/logs/event';


-- 装载数据
set hivevar:do_date= 2020-07-21;
alter table ods_log_event
    add partition (dt = '${do_date}');


-- 查询数据
select str,
       dt
from ods_log_event
where dt = '${do_date}'
limit 10;