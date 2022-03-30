-- 创建 路径分析

drop table if exists ads_page_path;

create external table ads_page_path
(
    `dt`          STRING COMMENT '统计日期',
    `recent_days` BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `source`      STRING COMMENT '跳转起始页面ID',
    `target`      STRING COMMENT '跳转终到页面ID',
    `path_count`  BIGINT COMMENT '跳转次数'
) comment '页面浏览路径分析'
    row format delimited fields terminated by '\t'
    location '/warehouse/gmall/ads/ads_page_path/';


-- 数据装载 路径分析

insert overwrite table ads_page_path
select *
from ads_page_path
union
select '2020-06-14' dt,
       recent_days,
       source,
       nvl(target, 'null'),
       count(*)     path_count
from (
         select recent_days,
                concat('step-', rn, ':', page_id)          source,
                concat('step-', rn + 1, ':', next_page_id) target
         from (
                  select recent_days,
                         page_id,
                         lead(page_id, 1, null) over (partition by session_id, recent_days)          next_page_id,
                         row_number() over (partition by session_id, recent_days order by view_time) rn
                  from dwd_traffic_page_view_inc
                           lateral view explode(array(1, 7, 30)) tmp as recent_days
                  where dt >= date_add('2020-06-14', -recent_days + 1)
              ) t1
     ) t2
group by recent_days, source, target;



-- 查看 路径分析

select dt,
       recent_days,
       source,
       target,
       path_count
from ads_page_path;







