-- 新增用户数
drop table if exists ads_new_member_cnt;

create table ads_new_member_cnt
(
    `dt` varchar(10) comment '统计日期',
    `cnt` string comment '用户数',
    primary key (dt)
);


select dt,
       cnt
from ads_new_member_cnt;