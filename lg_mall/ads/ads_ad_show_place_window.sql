-- 活动曝光前 100
drop table if exists ads_ad_show_place_window;

create table ads_ad_show_place_window
(
    hour       string comment '小时',
    place      string comment '位置',
    product_id int comment '商品 id',
    cnt        bigint comment '统计数',
    rank       int comment '排名'
) comment '活动曝光前 100'
    partitioned by (`dt` string)
    row format delimited fields terminated by ',';

----------------------

-- 装载数据

-- 活动曝光数排名
set hive.execution.engine = spark;
set hivever:do_date = 2020-07-21;
select hour,
       place,
       product_id,
       cnt,
       row_number() over (partition by hour, place, product_id order by cnt desc) rank
from ads_ad_show_place
where dt = '${do_date}'
  and ad_action = '0';



set hivever:do_date = 2020-07-21;
insert overwrite table ads_ad_show_place_window
    partition (dt = '${do_date}')
select *
from (
         select hour,
                place,
                product_id,
                cnt,
                row_number() over (partition by hour, place, product_id order by cnt desc) rank
         from ads_ad_show_place
         where dt = '${do_date}'
           and ad_action = '0'
     ) t
where rank <= 100;


--------------------


-- 查询数据
select hour,
       place,
       product_id,
       cnt,
       rank,
       dt
from ads_ad_show_place_window
where dt = '${do_date}'
limit 10;