-- 活动曝光详细
drop table if exists ads_ad_show_place;

create table ads_ad_show_place
(
    ad_action  tinyint comment '行为',
    hour       string comment '小时',
    place      string comment '位置',
    product_id int comment '商品id',
    cnt        bigint comment '统计数'
) comment '活动曝光详细'
    partitioned by (`dt` string)
    row format delimited fields terminated by ',';


-- 装载数据
set hive.execution.engine = spark;
set hivever:do_date = 2020-07-21;
insert overwrite table ads_ad_show_place
    partition (dt = '${do_date}')
select ad_action,
       hour,
       place,
       product_id,
       count(*)
from dwd_ad
where dt = '${do_date}'
group by ad_action, hour, place, product_id;


-- 查询数据
select ad_action,
       hour,
       place,
       product_id,
       cnt,
       dt
from ads_ad_show_place
where dt = '${do_date}'
limit 10;