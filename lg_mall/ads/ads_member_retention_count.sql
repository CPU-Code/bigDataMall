-- 用户留存数
drop table if exists ads_member_retention_count;

create table ads_member_retention_count
(
    `add_date`        string comment '新增日期',
    `retention_day`   int comment '截止当前日期留存天数',
    `retention_count` bigint comment '留存数'
) comment '用户留存数'
    partitioned by (`dt` string)
    row format delimited fields terminated by ',';


-- 装载数据
set hive.execution.engine = spark;
set hivevar:do_date= 2020-07-22;
insert overwrite table ads_member_retention_count
    partition (dt = '${do_date}')
select add_date,
       retention_date,
       count(*) retention_count
from dws_member_retention_day
where dt = '${do_date}'
group by add_date, retention_date;


-- 查询
select add_date,
       retention_day,
       retention_count,
       dt
from ads_member_retention_count
where dt = '${do_date}'