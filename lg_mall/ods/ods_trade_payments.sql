-- 支付方式表
drop table if exists ods_trade_payments;

create external table ods_trade_payments
(
    `id`          string comment 'id',
    `pay_method`  string comment '支付方式',
    `pay_name`    string comment '支付名',
    `description` string comment '备注',
    `pay_order`   int comment '支付订单',
    `online`      tinyint comment '在线状态'
) comment '支付方式表'
    partitioned by (`dt` string)
    row format delimited fields terminated by ','
    location '/origin_data/lg_mall/trade.db/payments/';

-- 加载数据
set hivevar:do_date = 2020-07-12;
alter table ods_trade_payments
    add partition (dt = '${do_date}');


-- 查询数据
set hivevar:do_date = 2020-07-12;
select id,
       pay_method,
       pay_name,
       description,
       pay_order,
       online,
       dt
from ods_trade_payments
where dt = '${do_date}'
limit 10;