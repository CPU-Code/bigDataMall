-- 创建 用户留存率

drop table if exists ads_user_retention;

create external table ads_user_retention
(
    `dt`              STRING COMMENT '统计日期',
    `create_date`     STRING COMMENT '用户新增日期',
    `retention_day`   INT COMMENT '截至当前日期留存天数',
    `retention_count` BIGINT COMMENT '留存用户数量',
    `new_user_count`  BIGINT COMMENT '新增用户数量',
    `retention_rate`  DECIMAL(16, 2) COMMENT '留存率'
) comment '用户留存率'
    row format delimited fields terminated by '\t'
    location '/warehouse/gmall/ads/ads_user_retention/';


-- 数据装载 用户留存率

insert overwrite table ads_user_retention
select *
from ads_user_retention
union
select '2020-06-14'                                                                           dt,
       login_date_first                                                                       create_date,
       datediff('2020-06-14', login_date_first)                                               retention_day,
       sum(if(login_date_last = '2020-06-14', 1, 0))                                          retention_count,
       count(*)                                                                               new_user_count,
       cast(sum(if(login_date_last = '2020-06-14', 1, 0)) / count(*) * 100 as decimal(16, 2)) retention_rate
from (
         select user_id,
                date_id login_date_first
         from dwd_user_register_inc
         where dt >= date_add('2020-06-14', -7)
           and dt < '2020-06-14'
     ) t1
         join (
    select user_id,
           login_date_last
    from dws_user_user_login_td
    where dt = '2020-06-14'
) t2
              on t1.user_id = t2.user_id
group by login_date_first;


-- 查看 用户留存率

select dt,
       create_date,
       retention_day,
       retention_count,
       new_user_count,
       retention_rate
from ads_user_retention;




