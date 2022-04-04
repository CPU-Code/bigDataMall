-- 创建 各省份订单统计

drop table if exists `ads_order_by_province`;

create table `ads_order_by_province`
(
    `dt`                 date                                                   NOT NULL COMMENT '统计日期',
    `recent_days`        bigint(20)                                             NOT NULL COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `province_id`        varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '省份ID',
    `province_name`      varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '省份名称',
    `area_code`          varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '地区编码',
    `iso_code`           varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '国际标准地区编码',
    `iso_code_3166_2`    varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '国际标准地区编码',
    `order_count`        bigint(20)                                             NULL DEFAULT NULL COMMENT '订单数',
    `order_total_amount` decimal(16, 2)                                         NULL DEFAULT NULL COMMENT '订单金额',
    PRIMARY KEY (`dt`, `recent_days`, `province_id`) USING BTREE
) engine = InnoDB
  character set = utf8
  collate = utf8_general_ci comment = '各地区订单统计'
  row_format = Dynamic;



