-- 删除 优惠券领用表
drop table if exists ods_coupon_use_inc;

-- 创建 优惠券领用表
create external table ods_coupon_use_inc
(
    `type` string comment '变动类型',
    `ts`   bigint comment '变动时间',
    `data` struct<id :string, coupon_id :string, user_id :string, order_id :string, coupon_status :string, get_time
                  :string, using_time :string, used_time :string, expire_time :string> comment '数据',
    `old`  map<string,string> comment '旧值'
) comment '优惠券领用表'
    partitioned by (`dt` string)
    row format serde 'org.apache.hadoop.hive.serde2.JsonSerDe'
    location '/warehouse/gmall/ods/ods_coupon_use_inc/';

-- 转载数据
load data inpath '/origin_data/gmall/db/coupon_use_inc/2020-06-14'
    into table ods_coupon_use_inc
    partition (dt = '2020-06-14');

-- 查询数据
select type,
       ts,
       data.id,
       data.coupon_id,
       data.user_id,
       data.order_id,
       data.coupon_status,
       data.get_time,
       data.using_time,
       data.used_time,
       data.expire_time,
       old,
       dt
from ods_coupon_use_inc
where dt = '2020-06-14';