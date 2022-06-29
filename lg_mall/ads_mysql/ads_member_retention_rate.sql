-- 用户留存率
drop table if exists ads_member_retention_rate;

create table ads_member_retention_rate
(
    `dt`              varchar(10) COMMENT '统计日期',
    `add_date` string comment '新增日期',
    `retention_day`   int comment '截止当前日期留存天数',
    `retention_count` bigint comment '留存数',
    `new_mid_count`   bigint comment '当日用户新增数',
    `retention_ratio` decimal(10, 2) comment '留存率',
    primary key (dt)
) comment '用户留存率';

-- 查询
select dt,
       add_date,
       retention_day,
       retention_count,
       new_mid_count,
       retention_ratio
from ads_member_retention_rate
where dt = '2020-06-21';