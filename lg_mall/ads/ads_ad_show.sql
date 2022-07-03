-- 广告点击次数分析
drop table if exists ads_ad_show;

create table ads_ad_show
(
    cnt        bigint comment '用户行为次数',
    u_cnt      bigint comment '用户id 数',
    device_cnt bigint comment '设备id 数',
    ad_action  tinyint comment '用户行为类型',
    hour       string comment '小时'
) comment '广告点击次数分析'
    partitioned by (`dt` string)
    row format delimited fields terminated by ',';


-- 装载数据
set hive.execution.engine = spark;
set hivevar:do_date = 2020-07-21;
insert overwrite table ads_ad_show
    partition (dt = '${do_date}')
select count(*),
       count(distinct uid),
       count(distinct device_id),
       ad_action,
       hour
from dwd_ad
where dt = '${do_date}'
group by ad_action, hour;


-- 查询数据
set hivevar:do_date = 2020-07-21;
select cnt,
       u_cnt,
       device_cnt,
       ad_action,
       hour,
       dt
from ads_ad_show
where dt = '${do_date}'
limit 10;