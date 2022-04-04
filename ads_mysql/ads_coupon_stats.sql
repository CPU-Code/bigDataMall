-- 各优惠券补贴率

drop table if exists `ads_coupon_stats`;

create table `ads_coupon_stats`
(
    `dt`          date                                                   NOT NULL COMMENT '统计日期',
    `coupon_id`   varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '优惠券ID',
    `coupon_name` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '优惠券名称',
    `start_date`  varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '发布日期',
    `rule_name`   varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '优惠规则，例如满100元减10元',
    `reduce_rate` decimal(16, 2)                                         NULL DEFAULT NULL COMMENT '补贴率',
    PRIMARY KEY (`dt`, `coupon_id`) USING BTREE
) engine = InnoDB
  character set = utf8
  collate = utf8_general_ci comment = '优惠券统计'
  row_format = Dynamic;

