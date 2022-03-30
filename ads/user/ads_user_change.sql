-- 创建 用户变动统计

drop table if exists ads_user_change;

create external table ads_user_change
(
    `dt`               STRING COMMENT '统计日期',
    `user_churn_count` BIGINT COMMENT '流失用户数',
    `user_back_count`  BIGINT COMMENT '回流用户数'
) comment '用户变动统计'
    row format delimited fields terminated by '\t'
    location '/warehouse/gmall/ads/ads_user_change/';


-- 数据装载 用户变动统计

insert overwrite table ads_user_change
select *
from ads_user_change
union
select churn.dt,
       user_churn_count,
       user_back_count
from (
         select '2020-06-14' dt,
                count(*)     user_churn_count
         from dws_user_user_login_td
         where dt = '2020-06-14'
           and login_date_last = date_add('2020-06-14', -7)
     ) churn
         join (
    select '2020-06-14' dt,
           count(*)     user_back_count
    from (
             select user_id,
                    login_date_last
             from dws_user_user_login_td
             where dt = '2020-06-14'
         ) t1
             join (
        select user_id,
               login_date_last login_date_previous
        from dws_user_user_login_td
        where dt = date_add('2020-06-14', -1)
    ) t2
                  on t1.user_id = t2.user_id
    where datediff(login_date_last, login_date_previous) >= 8
) back
              on churn.dt = back.dt;


-- 查看 用户变动统计

select dt,
       user_churn_count,
       user_back_count
from ads_user_change;






