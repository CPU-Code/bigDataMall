-- 删除 优惠券信息表
drop table if exists ods_coupon_info_full;

-- 创建 优惠券信息表
create external table ods_coupon_info_full
(
    `id`               string comment '购物券编号',
    `coupon_name`      string comment '购物券名称',
    `coupon_type`      string comment '购物券类型 1 现金券 2 折扣券 3 满减券 4 满件打折券',
    `condition_amount` DECIMAL(16, 2) COMMENT '满额数',
    `condition_num`    BIGINT COMMENT '满件数',
    `activity_id`      STRING COMMENT '活动编号',
    `benefit_amount`   DECIMAL(16, 2) COMMENT '减金额',
    `benefit_discount` DECIMAL(16, 2) COMMENT '折扣',
    `create_time`      STRING COMMENT '创建时间',
    `range_type`       STRING COMMENT '范围类型 1、商品 2、品类 3、品牌',
    `limit_num`        BIGINT COMMENT '最多领用次数',
    `taken_count`      BIGINT COMMENT '已领用次数',
    `start_time`       STRING COMMENT '开始领取时间',
    `end_time`         STRING COMMENT '结束领取时间',
    `operate_time`     STRING COMMENT '修改时间',
    `expire_time`      STRING COMMENT '过期时间'
) comment '优惠券信息表'
    partitioned by (`dt` string)
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/warehouse/gmall/ods/ods_coupon_info_full/';


-- 装载数据
load data inpath '/origin_data/gmall/db/coupon_info_full/2020-06-14'
    into table ods_coupon_info_full
    partition (dt = '2020-06-14');

-- 查询数据
select id,
       coupon_name,
       coupon_type,
       condition_amount,
       condition_num,
       activity_id,
       benefit_amount,
       benefit_discount,
       create_time,
       range_type,
       limit_num,
       taken_count,
       start_time,
       end_time,
       operate_time,
       expire_time,
       dt
from ods_coupon_info_full
where dt = '2020-06-14';