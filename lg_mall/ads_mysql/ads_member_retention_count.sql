-- 用户留存数

drop table if exists ads_member_retention_count;

create table ads_member_retention_count
(
    `dt`              varchar(10) comment '统计日期',
    `add_date` string comment '新增日期',
    `retention_day`   int comment '截止当前日期留存天数',
    `retention_count` bigint comment '留存数',
    primary key (dt)
) comment '用户留存情况';


-- 查询
select dt,
       add_date,
       retention_day,
       retention_count
from ads_member_retention_count