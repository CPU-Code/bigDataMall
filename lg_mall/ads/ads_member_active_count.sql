-- 删除 活跃会员数
drop table if exists ads_member_active_count;

-- 创建 活跃会员数
create table ads_member_active_count
(
    `day_count`   int comment '当日用户数量',
    `week_count`  int comment '当周用户数量',
    `month_count` int comment '当月用户数量'
) comment '活跃用户数'
    partitioned by (dt string)
    row format delimited fields terminated by ',';


-- 查询 活跃会员数

-- union all 连接数据
set hive.execution.engine = spark;
select 'day'    datelable,
       count(*) cnt,
       dt
from dws_member_start_day
where dt = '2020-07-21'
group by dt
union all
select 'week'   datelabel,
       count(*) cnt,
       dt
from dws_member_start_week
where dt = '2020-07-21'
group by dt
union all
select 'month'  datelabel,
       count(*) cnt,
       dt
from dws_member_start_month
where dt = '2020-07-21'
group by dt;


-- 装载数据
set hive.execution.engine = spark;
with tmp as (
    select 'day'    datelabel,
           count(*) cnt,
           dt
    from dws_member_start_day
    where dt = '2020-07-21'
    group by dt
    union all
    select 'week'   datelabel,
           count(*) cnt,
           dt
    from dws_member_start_week
    where dt = '2020-07-21'
    group by dt
    union all
    select 'month'  datelabel,
           count(*) cnt,
           dt
    from dws_member_start_month
    where dt = '2020-07-21'
    group by dt
)
insert
overwrite
table
ads_member_active_count
partition
(
dt = '2020-07-21'
)
select sum(case when datelabel = 'day' then cnt end)   as day_count,
       sum(case when datelabel = 'week' then cnt end)  as
                                                          week_count,
       sum(case when datelabel = 'month' then cnt end) as
                                                          month_count
from tmp
group by dt;


-- 查询数据
select day_count,
       week_count,
       month_count,
       dt
from ads_member_active_count
where dt = '2020-07-21';


-- 另外一种方法


-- 连接数据
set hive.execution.engine = spark;
select *
from (select dt,
             count(*) daycnt
      from dws_member_start_day
      where dt = '2020-07-21'
      group by dt) as `day`
         join (
    select dt,
           count(*) weekcnt
    from dws_member_start_week
    where dt = '2020-07-21'
    group by dt
) as `week`
              on `day`.dt = `week`.dt
         join(
    select dt,
           count(*) monthcnt
    from dws_member_start_month
    where dt = '2020-07-21'
    group by dt
) as month
             on day.dt = month.dt;


-- 装载数据
set hive.execution.engine = spark;
insert overwrite table ads_member_active_count
    partition (dt = '2020-07-21')
select daycnt,
       weekcnt,
       monthcnt
from (select dt,
             count(*) daycnt
      from dws_member_start_day
      where dt = '2020-07-21'
      group by dt) as `day`
         join (
    select dt,
           count(*) weekcnt
    from dws_member_start_week
    where dt = '2020-07-21'
    group by dt
) as `week`
              on `day`.dt = `week`.dt
         join(
    select dt,
           count(*) monthcnt
    from dws_member_start_month
    where dt = '2020-07-21'
    group by dt
) as month
             on day.dt = month.dt;

-- 查询数据
select day_count,
       week_count,
       month_count,
       dt
from ads_member_active_count
where dt = '2020-07-21';

