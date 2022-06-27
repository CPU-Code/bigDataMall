-- 删除用户启动日志
drop table if exists ods_start_log;

-- 创建用户启动日志表
create external table ods_start_log
(
    `str` string comment 'json 数据'
) comment '用户启动日志信息'
    partitioned by (`dt` string)
    location '/user/yx_test/data/logs/start';


-- 加载数据
alter table ods_start_log
    add partition (dt = '2020-07-22');


select str,
       dt
from ods_start_log
where dt = '2020-07-22';