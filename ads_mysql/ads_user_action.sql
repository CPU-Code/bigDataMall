-- 用户行为漏斗分析

drop table if exists `ads_user_action`;

create table `ads_user_action`
(
    `dt`                date       NOT NULL COMMENT '统计日期',
    `recent_days`       bigint(20) NOT NULL COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `home_count`        bigint(20) NULL DEFAULT NULL COMMENT '浏览首页人数',
    `good_detail_count` bigint(20) NULL DEFAULT NULL COMMENT '浏览商品详情页人数',
    `cart_count`        bigint(20) NULL DEFAULT NULL COMMENT '加入购物车人数',
    `order_count`       bigint(20) NULL DEFAULT NULL COMMENT '下单人数',
    `payment_count`     bigint(20) NULL DEFAULT NULL COMMENT '支付人数',
    PRIMARY KEY (`dt`, `recent_days`) USING BTREE
) engine = InnoDB
  character set = utf8
  collate = utf8_general_ci comment = '漏斗分析'
  row_format = Dynamic;