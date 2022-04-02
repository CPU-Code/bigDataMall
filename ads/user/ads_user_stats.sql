-- 创建 用户新增活跃统计

drop table if exists ads_user_status;

create external table ads_user_stats
(
    `dt`                STRING COMMENT '统计日期',
    `recent_days`       BIGINT COMMENT '最近n日,1:最近1日,7:最近7日,30:最近30日',
    `new_user_count`    BIGINT COMMENT '新增用户数',
    `active_user_count` BIGINT COMMENT '活跃用户数'
) comment '用户新增活跃统计'
    row format delimited fields terminated by '\t'
    location '/warehouse/gmall/ads/ads_user_stats/';


-- 数据装载 用户新增活跃统计

insert overwrite table ads_user_stats
select *
from ads_user_stats
union
select '2020-06-14' dt,
       t1.recent_days,
       new_user_count,
       active_user_count
from (
         select recent_days,
                sum(if(login_date_last >= date_add('2020-06-14', -recent_days + 1), 1, 0)) active_user_count
         from dws_user_user_login_td lateral view explode(array(1, 7, 30)) tmp as recent_days
         where dt = '2020-06-14'
         group by tmp.recent_days
     ) t1
         join (
    select recent_days,
           sum(if(date_id >= date_add('2020-06-14', -recent_days + 1), 1, 0)) new_user_count
    from dwd_user_register_inc lateral view explode(array(1, 7, 30)) tmp as recent_days
    group by recent_days
) t2
              on t1.recent_days = t2.recent_days;


-- 查看 用户新增活跃统计

select dt,
       recent_days,
       new_user_count,
       active_user_count
from ads_user_stats;

