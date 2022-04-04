-- 新增交易用户统计

drop table if exists `ads_new_buyer_stats`;

create table `ads_new_buyer_stats`
(
    `dt`                     date       NOT NULL COMMENT '统计日期',
    `recent_days`            bigint(20) NOT NULL COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `new_order_user_count`   bigint(20) NULL DEFAULT NULL COMMENT '新增下单人数',
    `new_payment_user_count` bigint(20) NULL DEFAULT NULL COMMENT '新增支付人数',
    PRIMARY KEY (`dt`, `recent_days`) USING BTREE
) engine = InnoDB
  character set = utf8
  collate = utf8_general_ci comment = '新增交易用户统计'
  row_format = Dynamic;