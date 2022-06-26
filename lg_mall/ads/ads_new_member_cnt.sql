drop table if exists ads_new_member_cnt;

-- 创建新增用户数
create table ads_new_member_cnt
(
    `cnt` string
) comment '新增用户数'
    partitioned by (`dt` string)
    row format delimited fields terminated by ',';


-- 装载数据

insert overwrite table ads_new_member_cnt
    partition (dt = '2020-07-21')
select count(1)
from dws_member_add_day
where dt = '2020-07-21';


-- 查询数据

select cnt,
       dt
from ads_new_member_cnt
where dt = '2020-07-21';