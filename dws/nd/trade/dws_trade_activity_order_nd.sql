-- 创建 交易域活动粒度订单最近n日汇总表

drop table if exists dws_trade_activity_order_nd;

create external table dws_trade_activity_order_nd
(
    `activity_id`                STRING COMMENT '活动id',
    `activity_name`              STRING COMMENT '活动名称',
    `activity_type_code`         STRING COMMENT '活动类型编码',
    `activity_type_name`         STRING COMMENT '活动类型名称',
    `start_date`                 STRING COMMENT '发布日期',
    `original_amount_30d`        DECIMAL(16, 2) COMMENT '参与活动订单原始金额',
    `activity_reduce_amount_30d` DECIMAL(16, 2) COMMENT '参与活动订单优惠金额'
) comment '交易域活动粒度订单最近n日汇总事实表'
    partitioned by (`dt` string)
    stored as orc
    location '/warehouse/gmall/dws/dws_trade_activity_order_nd'
    tblproperties ('orc.compress' = 'snappy');


-- 每日装载 交易域活动粒度订单最近n日汇总表

insert overwrite table dws_trade_activity_order_nd
    partition (dt = '2020-06-14')
select act.activity_id,
       activity_name,
       activity_type_code,
       activity_type_name,
       date_format(start_time, 'yyyy-MM-dd'),
       sum(split_original_amount),
       sum(split_activity_amount)
from (
         select activity_id,
                activity_name,
                activity_type_code,
                activity_type_name,
                start_time
         from dim_activity_full
         where dt = '2020-06-14'
           and date_format(start_time, 'yyyy-MM-dd') >= date_add('2020-06-14', -29)
         group by activity_id, activity_name, activity_type_code, activity_type_name, start_time
     ) act
         left join (
    select activity_id,
           order_id,
           split_original_amount,
           split_activity_amount
    from dwd_trade_order_detail_inc
    where dt >= date_add('2020-06-14', -29)
      and dt <= '2020-06-14'
      and activity_id is not null
) od
                   on act.activity_id = od.activity_id
group by act.activity_id, activity_name, activity_type_code, activity_type_name, start_time;


-- 查看分区

show partitions dws_trade_activity_order_nd;


-- 查看 交易域活动粒度订单最近n日汇总表

select activity_id,
       activity_name,
       activity_type_code,
       activity_type_name,
       start_date,
       original_amount_30d,
       activity_reduce_amount_30d,
       dt
from dws_trade_activity_order_nd
where dt = '2020-06-14';



