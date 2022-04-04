-- 各品牌商品交易统计

drop table if exists `ads_trade_stats_by_tm`;

create table `ads_trade_stats_by_tm`
(
    `dt`                      date                                                   NOT NULL COMMENT '统计日期',
    `recent_days`             bigint(20)                                             NOT NULL COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `tm_id`                   varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '品牌ID',
    `tm_name`                 varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '品牌名称',
    `order_count`             bigint(20)                                             NULL DEFAULT NULL COMMENT '订单数',
    `order_user_count`        bigint(20)                                             NULL DEFAULT NULL COMMENT '订单人数',
    `order_refund_count`      bigint(20)                                             NULL DEFAULT NULL COMMENT '退单数',
    `order_refund_user_count` bigint(20)                                             NULL DEFAULT NULL COMMENT '退单人数',
    PRIMARY KEY (`dt`, `recent_days`, `tm_id`) USING BTREE
) engine = InnoDB
  character set = utf8
  collate = utf8_general_ci comment = '各品牌商品交易统计'
  row_format = Dynamic;