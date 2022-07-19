-- 订单事实表(拉链表)
drop table if exists dwd_trade_orders;

create table dwd_trade_orders
(
    `order_id`      int comment '订单 id',
    `order_no`      string comment '订单编号',
    `user_id`       bigint comment '用户id',
    `status`        tinyint comment '订单状态 : -3:用户拒收 -2:未付款的订单 -1:用户取消 0:待发货 1:配送中 2:用户确认收货',
    `product_money` decimal comment '商品金额',
    `total_money`   decimal comment '订单金额 ( 包括运费 )',
    `pay_method`    tinyint comment '支付方式: 0:未知 1:支付宝 2：微信 3: 现金 4: 其他',
    `is_pay`        tinyint comment '是否支付 0:未支付 1:已支付',
    `area_id`       int comment '区域最低一级',
    `trade_src`     tinyint comment '订单来源 : 0:商城 1:微信 2:手机版 3:安卓App 4:苹果App',
    `trade_type`    int comment '订单类型',
    `is_refund`     tinyint comment '是否退款 0:否 1：是',
    `data_flag`     tinyint comment '订单有效标志 : -1: 删除 1: 有效',
    `create_time`   string comment '下单时间',
    `pay_time`      string comment '支付时间',
    `modified_time` string comment '订单更新时间',
    `start_date`    string comment '拉链表起始',
    `end_date`      string comment '拉链表截止'
) comment '订单事实拉链表'
    partitioned by (dt string)
    stored as parquet;


------------------

-- 时间日期格式转换
-- 'yyyy-MM-dd HH:mm:ss' => timestamp => 'yyyy-MM-dd'

select modified_time
from ods_trade_orders
limit 10;

select unix_timestamp(modified_time, 'yyyy-MM-dd HH:mm:ss')
from ods_trade_orders
limit 10;


select from_unixtime(unix_timestamp(modified_time, 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd')
from ods_trade_orders
limit 10;


-- 查看新增数据
set hivevar:do_date = 2020-07-12;
set hive.execution.engine = spark;
select order_id,
       order_no,
       user_id,
       status,
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
       case
           when modified_time is not null
               then from_unixtime(unix_timestamp(modified_time,
                                                 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd')
           else from_unixtime(unix_timestamp(create_time,
                                             'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd')
           end                                                                         as start_date,
       '9999-12-31'                                                                    as end_date,
       from_unixtime(unix_timestamp(create_time, 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd') as dt
from ods_trade_orders
where dt = '${do_date}'
limit 10;



-- 查看修改和历史数据
set hivevar:do_date = 2020-07-12;
set hive.execution.engine = spark;
select t1.order_id,
       t1.order_no,
       t1.user_id,
       t1.status,
       t1.product_money,
       t1.total_money,
       t1.pay_method,
       t1.is_pay,
       t1.area_id,
       t1.trade_src,
       t1.trade_type,
       t1.is_refund,
       t1.data_flag,
       t1.create_time,
       t1.pay_time,
       t1.modified_time,
       t1.start_date,
       case
           when t2.order_id is not null and t1.end_date > '${do_date}'
               then date_add('${do_date}', -1)
           else t1.end_date end                                                           as end_date,
       from_unixtime(unix_timestamp(t1.create_time, 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd') as dt
from (
         select *
         from dwd_trade_orders
         where dt > date_add('$do_date', -15)
     ) t1
         left outer join (
    select order_id
    from ods_trade_orders
    where dt = '${do_date}'
) t2 on t1.order_id = t2.order_id
limit 10;




-- 装载数据
set hivevar:do_date = 2020-07-12;
set hive.execution.engine = spark;
set hive.exec.dynamic.partition.mode = nonstrict;
set hive.exec.dynamic.partition=true;
insert overwrite table dwd_trade_orders
    partition (dt)
select order_id,
       order_no,
       user_id,
       status,
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
       case
           when modified_time is not null
               then from_unixtime(unix_timestamp(modified_time,
                                                 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd')
           else from_unixtime(unix_timestamp(create_time,
                                             'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd')
           end                                                                         as start_date,
       '9999-12-31'                                                                    as end_date,
       from_unixtime(unix_timestamp(create_time, 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd') as dt
from ods_trade_orders
where dt = '${do_date}'
union all
select t1.order_id,
       t1.order_no,
       t1.user_id,
       t1.status,
       t1.product_money,
       t1.total_money,
       t1.pay_method,
       t1.is_pay,
       t1.area_id,
       t1.trade_src,
       t1.trade_type,
       t1.is_refund,
       t1.data_flag,
       t1.create_time,
       t1.pay_time,
       t1.modified_time,
       t1.start_date,
       case
           when t2.order_id is not null and t1.end_date > '${do_date}'
               then date_add('${do_date}', -1)
           else t1.end_date end                                                           as end_date,
       from_unixtime(unix_timestamp(t1.create_time, 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd') as dt
from (
         select *
         from dwd_trade_orders
         where dt > date_add('${do_date}', -15)
     ) t1
         left outer join (
    select order_id
    from ods_trade_orders
    where dt = '${do_date}'
) t2 on t1.order_id = t2.order_id;


--------------------

-- 查询数据
set hivevar:do_date = 2020-06-28;
select order_id,
       order_no,
       user_id,
       status,
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
       start_date,
       end_date,
       dt
from dwd_trade_orders
where dt = '${do_date}'
limit 10;