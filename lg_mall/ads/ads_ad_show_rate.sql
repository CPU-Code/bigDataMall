-- 点击率购买率
drop table if exists ads_ad_show_rate;

create table ads_ad_show_rate
(
    hour       string comment '小时',
    click_rate double comment '点击率',
    buy_rate   double comment '购买率'
) comment '点击率购买率'
    partitioned by (`dt` string)
    row format delimited fields terminated by ',';


-- 装载数据

set hive.execution.engine = spark;
set hivevar:do_date = 2020-07-21;
select max(case when ad_action = '0' then cnt end) show_cnt,
       max(case when ad_action = '1' then cnt end) click_cnt,
       max(case when ad_action = '2' then cnt end) buy_cnt,
       hour
from ads_ad_show
where dt = '${do_date}'
group by hour
limit 10;


set hive.execution.engine = spark;
set hivevar:do_date = 2020-07-21;
with tmp as (
    select max(case when ad_action = '0' then cnt end) show_cnt,
           max(case when ad_action = '1' then cnt end) click_cnt,
           max(case when ad_action = '2' then cnt end) buy_cnt,
           hour
    from ads_ad_show
    where dt = '${do_date}'
    group by hour
)
insert
overwrite
table
ads_ad_show_rate
partition
(
dt = '${do_date}'
)
select hour,
       click_cnt / show_cnt as click_rate,
       buy_cnt / click_cnt  as buy_rate
from tmp;


-- 查询数据
select hour,
       click_rate,
       buy_rate,
       dt
from ads_ad_show_rate
where dt = '${do_date}'
limit 10;