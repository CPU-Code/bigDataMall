-- 创建订单表
drop table if exists ods_trade_orders;

create external table ods_trade_orders
(
    `orderid`      int comment '订单 id',
    `orderno`      string comment '订单编号',
    `userid`       bigint comment '用户id',
    `status`       tinyint comment '订单状态 : -3:用户拒收 -2:未付款的订单 -1:用户取消 0:待发货 1:配送中 2:用户确认收货',
    `productmoney` decimal(10, 0) comment '商品金额',
    `totalmoney`   decimal(10, 0) comment '订单金额（包括运费）',
    `paymethod`    tinyint comment '支付方式: 0:未知 1:支付宝 2：微信 3: 现金 4: 其他',
    `ispay`        tinyint comment '是否支付 0:未支付 1:已支付',
    `areaid`       int comment '区域最低一级',
    `tradesrc`     tinyint comment '订单来源 : 0:商城 1:微信 2:手机版 3:安卓App 4:苹果App',
    `tradetype`    int comment '订单类型',
    `isrefund`     tinyint comment '是否退款 0:否 1：是',
    `dataflag`     tinyint comment '订单有效标志 : -1: 删除 1: 有效',
    `createtime`   string comment '下单时间',
    `paytime`      string comment '支付时间',
    `modifiedtime` string comment '订单更新时间'
) comment '订单表'
    partitioned by (`dt` string)
    row format delimited fields terminated by ','
    location '/origin_data/lg_mall/trade.db/orders/';


-- 装载数据
set hivevar:do_date = 2020-07-12;
alter table ods_trade_orders
    add partition (dt = '${do_date}');


-- 查询
set hive.execution.engine = spark;
set hivevar:do_date = 2020-07-12;
select order_id,
       order_no,
       user_id,
       `status`,
       product_money,
       total_money,
       pay_method,
       is_pay,
       area_id,
       trade_src,
       trade_type,
       is_refund,
       data_flag,
       create_time,
       pay_time,
       modified_time,
       dt
from ods_trade_orders
where dt = '${do_date}';

