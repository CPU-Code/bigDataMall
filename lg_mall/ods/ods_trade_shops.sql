-- 商家店铺表
drop table if exists `ods_trade_shops`;

create external table `ods_trade_shops`
(
    `shop_id`     int comment '商铺ID',
    `user_id`     int comment '商铺联系人ID',
    `area_id`     int comment '地址id',
    `shop_name`   string comment '商铺名称',
    `shop_level`  tinyint comment '店铺等级',
    `status`      tinyint comment '商铺状态',
    `create_time` string comment '创建时间',
    `modify_time` string comment '修改时间'
) comment '商家店铺表'
    partitioned by (`dt` string)
    row format delimited fields terminated by ','
    location '/origin_data/lg_mall/trade.db/shops';


-- 装载数据
set hivevar:do_date = 2020-07-12;
alter table ods_trade_shops
    add partition (dt = '${do_date}');


-- 查询数据
set hivevar:do_date = 2020-07-12;
select shop_id,
       user_id,
       area_id,
       shop_name,
       shop_level,
       status,
       create_time,
       modify_time,
       dt
from ods_trade_shops
where dt = '${do_date}'
limit 10;
