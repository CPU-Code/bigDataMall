-- 各品类商品交易统计

drop table if exists `ads_trade_stats_by_cate`;

create table `ads_trade_stats_by_cate`
(
    `dt`                      date                                                   NOT NULL COMMENT '统计日期',
    `recent_days`             bigint(20)                                             NOT NULL COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `category1_id`            varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '一级分类id',
    `category1_name`          varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '一级分类名称',
    `category2_id`            varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '二级分类id',
    `category2_name`          varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '二级分类名称',
    `category3_id`            varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '三级分类id',
    `category3_name`          varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '三级分类名称',
    `order_count`             bigint(20)                                             NULL DEFAULT NULL COMMENT '订单数',
    `order_user_count`        bigint(20)                                             NULL DEFAULT NULL COMMENT '订单人数',
    `order_refund_count`      bigint(20)                                             NULL DEFAULT NULL COMMENT '退单数',
    `order_refund_user_count` bigint(20)                                             NULL DEFAULT NULL COMMENT '退单人数',
    PRIMARY KEY (`dt`, `recent_days`, `category1_id`, `category2_id`, `category3_id`) USING BTREE
) engine = InnoDB
  character set = utf8
  collate = utf8_general_ci comment = '各分类商品交易统计'
  row_format = Dynamic;
