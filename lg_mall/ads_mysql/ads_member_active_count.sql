drop table if exists ads_member_active_count;

-- 活跃用户数
create table ads_member_active_count
(
    `dt`          varchar(10) comment '统计日期',
    `day_count`   int comment '当日用户数量',
    `week_count`  int comment '当周用户数量',
    `month_count` int comment '当月用户数量',
    primary key (dt)
);

select dt,
       day_count,
       week_count,
       month_count
from ads_member_active_count
where dt = '2020-06-21';

