-- 创建 交易域省份粒度订单最近1日汇总表

drop table if exists dws_trade_province_order_1d;

create external table dws_trade_province_order_1d
(
    `province_id`               STRING COMMENT '用户id',
    `province_name`             STRING COMMENT '省份名称',
    `area_code`                 STRING COMMENT '地区编码',
    `iso_code`                  STRING COMMENT '旧版ISO-3166-2编码',
    `iso_3166_2`                STRING COMMENT '新版版ISO-3166-2编码',
    `order_count_1d`            BIGINT COMMENT '最近1日下单次数',
    `order_original_amount_1d`  DECIMAL(16, 2) COMMENT '最近1日下单原始金额',
    `activity_reduce_amount_1d` DECIMAL(16, 2) COMMENT '最近1日下单活动优惠金额',
    `coupon_reduce_amount_1d`   DECIMAL(16, 2) COMMENT '最近1日下单优惠券优惠金额',
    `order_total_amount_1d`     DECIMAL(16, 2) COMMENT '最近1日下单最终金额'
) comment '交易域省份粒度订单最近1日汇总事实表'
    partitioned by (`dt` string)
    stored as orc
    location '/warehouse/gmall/dws/dws_trade_province_order_1d'
    tblproperties ('orc.compress' = 'snappy');


-- 首日装载 交易域省份粒度订单最近1日汇总表

set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table dws_trade_province_order_1d
    partition (dt)
select province_id,
       province_name,
       area_code,
       iso_code,
       iso_3166_2,
       order_count_1d,
       order_original_amount_1d,
       activity_reduce_amount_1d,
       coupon_reduce_amount_1d,
       order_total_amount_1d,
       dt
from (
         select province_id,
                count(distinct (order_id))         order_count_1d,
                sum(split_original_amount)         order_original_amount_1d,
                sum(nvl(split_activity_amount, 0)) activity_reduce_amount_1d,
                sum(nvl(split_coupon_amount, 0))   coupon_reduce_amount_1d,
                sum(split_total_amount)            order_total_amount_1d,
                dt
         from dwd_trade_order_detail_inc
         group by province_id, dt
     ) o
         left join (
    select id,
           province_name,
           area_code,
           iso_code,
           iso_3166_2
    from dim_province_full
    where dt = '2020-06-14'
) p
                   on o.province_id = p.id;


-- 每日装载 交易域省份粒度订单最近1日汇总表
insert overwrite table dws_trade_province_order_1d
    partition (dt = '2020-06-15')
select province_id,
       province_name,
       area_code,
       iso_code,
       iso_3166_2,
       order_count_1d,
       order_original_amount_1d,
       activity_reduce_amount_1d,
       coupon_reduce_amount_1d,
       order_total_amount_1d
from (
         select province_id,
                count(distinct (order_id))         order_count_1d,
                sum(split_original_amount)         order_original_amount_1d,
                sum(nvl(split_activity_amount, 0)) activity_reduce_amount_1d,
                sum(nvl(split_coupon_amount, 0))   coupon_reduce_amount_1d,
                sum(split_total_amount)            order_total_amount_1d,
                dt
         from dwd_trade_order_detail_inc
         where dt = '2020-06-15'
         group by province_id
     ) o
         left join (
    select id,
           province_name,
           area_code,
           iso_code,
           iso_3166_2
    from dim_province_full
    where dt = '2020-06-15'
) p
                   on o.province_id = p.id;


-- 查看分区
show partitions dws_trade_province_order_1d;



-- 查看 交易域省份粒度订单最近1日汇总表

select province_id,
       province_name,
       area_code,
       iso_code,
       iso_3166_2,
       order_count_1d,
       order_original_amount_1d,
       activity_reduce_amount_1d,
       coupon_reduce_amount_1d,
       order_total_amount_1d,
       dt
from dws_trade_province_order_1d
where dt = '2020-06-14';





