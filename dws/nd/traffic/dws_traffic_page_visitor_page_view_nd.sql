-- 创建 流量域访客页面粒度页面浏览最近n日汇总表

drop table if exists dws_traffic_page_visitor_page_view_nd;

create external table dws_traffic_page_visitor_page_view_nd
(
    `mid_id`          STRING COMMENT '访客id',
    `brand`           string comment '手机品牌',
    `model`           string comment '手机型号',
    `operate_system`  string comment '操作系统',
    `page_id`         STRING COMMENT '页面id',
    `during_time_7d`  BIGINT COMMENT '最近7日浏览时长',
    `view_count_7d`   BIGINT COMMENT '最近7日访问次数',
    `during_time_30d` BIGINT COMMENT '最近30日浏览时长',
    `view_count_30d`  BIGINT COMMENT '最近30日访问次数'
) comment '流量域访客页面粒度页面浏览最近n日汇总事实表'
    partitioned by (`dt` string)
    stored as orc
    location '/warehouse/gmall/dws/dws_traffic_page_visitor_page_view_nd'
    tblproperties ('orc.compress' = 'snappy');


-- 每日装载 流量域访客页面粒度页面浏览最近n日汇总表

insert overwrite table dws_traffic_page_visitor_page_view_nd
    partition (dt = '2020-06-14')
select mid_id,
       brand,
       model,
       operate_system,
       page_id,
       sum(if(dt >= date_add('2020-06-14', -6), during_time_1d, 0)),
       sum(if(dt >= date_add('2020-06-14', -6), view_count_1d, 0)),
       sum(during_time_1d),
       sum(view_count_1d)
from dws_traffic_page_visitor_page_view_1d
where dt >= date_add('2020-06-14', -29)
  and dt <= '2020-06-14'
group by mid_id, brand, model, operate_system, page_id;


-- 查看分区

show partitions dws_traffic_page_visitor_page_view_nd;

-- 查看 流量域访客页面粒度页面浏览最近n日汇总表

select mid_id,
       brand,
       model,
       operate_system,
       page_id,
       during_time_7d,
       view_count_7d,
       during_time_30d,
       view_count_30d,
       dt
from dws_traffic_page_visitor_page_view_nd
where dt = '2020-06-14';




