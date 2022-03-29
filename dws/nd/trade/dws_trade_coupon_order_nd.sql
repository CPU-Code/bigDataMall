-- 创建 交易域优惠券粒度订单最近n日汇总表

drop table if exists dws_trade_coupon_order_nd;

create external table dws_trade_coupon_order_nd
(
    `coupon_id`                STRING COMMENT '优惠券id',
    `coupon_name`              STRING COMMENT '优惠券名称',
    `coupon_type_code`         STRING COMMENT '优惠券类型id',
    `coupon_type_name`         STRING COMMENT '优惠券类型名称',
    `coupon_rule`              STRING COMMENT '优惠券规则',
    `start_date`               STRING COMMENT '发布日期',
    `original_amount_30d`      DECIMAL(16, 2) COMMENT '使用下单原始金额',
    `coupon_reduce_amount_30d` DECIMAL(16, 2) COMMENT '使用下单优惠金额'
) comment '交易域优惠券粒度订单最近n日汇总事实表'
    partitioned by (`dt` string)
    stored as orc
    location '/warehouse/gmall/dws/dws_trade_coupon_order_nd'
    tblproperties ('orc.compress' = 'snappy');


-- 数据装载 交易域优惠券粒度订单最近n日汇总表

insert overwrite table dws_trade_coupon_order_nd
    partition (dt = '2020-06-14')
select id,
       coupon_name,
       coupon_type_code,
       coupon_type_name,
       benefit_rule,
       start_date,
       sum(split_original_amount),
       sum(split_coupon_amount)
from (
         select id,
                coupon_name,
                coupon_type_code,
                coupon_type_name,
                benefit_rule,
                date_format(start_time, 'yyyy-MM-dd') start_date
         from dim_coupon_full
         where dt = '2020-06-14'
           and date_format(start_time, 'yyyy-MM-dd') >= date_add('2020-06-14', -29)
     ) cou
         left join (
    select coupon_id,
           order_id,
           split_original_amount,
           split_coupon_amount
    from dwd_trade_order_detail_inc
    where dt >= date_add('2020-06-14', -29)
      and dt <= '2020-06-14'
      and coupon_id is not null
) od
                   on cou.id = od.coupon_id
group by id, coupon_name, coupon_type_code, coupon_type_name, benefit_rule, start_date;


-- 查看分区

show partitions dws_trade_coupon_order_nd;

-- 查看 交易域优惠券粒度订单最近n日汇总表

select coupon_id,
       coupon_name,
       coupon_type_code,
       coupon_type_name,
       coupon_rule,
       start_date,
       original_amount_30d,
       coupon_reduce_amount_30d
from dws_trade_coupon_order_nd
where dt = '2020-06-14';

