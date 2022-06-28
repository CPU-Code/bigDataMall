-- 会员留存率
drop table if exists ads_member_retention_rate;

create table ads_member_retention_rate
(
    `add_date`        string comment '新增日期',
    `retention_day`   int comment '截止当前日期留存天数',
    `retention_count` bigint comment '留存数',
    `new_mid_count`   bigint comment '当日会员新增数',
    `retention_ratio` decimal(10, 2) comment '留存率'
) comment '会员留存率'
    partitioned by (`dt` string)
    row format delimited fields terminated by ',';


-- 装载数据
set hive.execution.engine = spark;
set hivevar:do_date= 2020-07-22;
insert overwrite table ads_member_retention_rate
    partition (dt = '${do_date}')
select t1.add_date,
       t1.retention_day,
       t1.retention_count,
       t2.cnt,
       t1.retention_count / t2.cnt * 100
from ads_member_retention_count t1
         join ads_new_member_cnt t2
              on t1.dt = t2.dt
where t1.dt = '${do_date}';


-- 查询数据
select add_date,
       retention_day,
       retention_count,
       new_mid_count,
       retention_ratio,
       dt
from ads_member_retention_rate
where dt = '${do_date}';