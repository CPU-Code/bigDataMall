-- 创建 交易综合统计

drop table if exists `ads_trade_stats`;

create table `ads_trade_stats`
(
    `dt`                      date           NOT NULL COMMENT '统计日期',
    `recent_days`             bigint(255)    NOT NULL COMMENT '最近天数,1:最近1日,7:最近7天,30:最近30天',
    `order_total_amount`      decimal(16, 2) NULL DEFAULT NULL COMMENT '订单总额,GMV',
    `order_count`             bigint(20)     NULL DEFAULT NULL COMMENT '订单数',
    `order_user_count`        bigint(20)     NULL DEFAULT NULL COMMENT '下单人数',
    `order_refund_count`      bigint(20)     NULL DEFAULT NULL COMMENT '退单数',
    `order_refund_user_count` bigint(20)     NULL DEFAULT NULL COMMENT '退单人数',
    PRIMARY KEY (`dt`, `recent_days`) USING BTREE
) engine = InnoDB
  character set = utf8
  collate = utf8_general_ci comment = '交易统计'
  row_format = Dynamic;