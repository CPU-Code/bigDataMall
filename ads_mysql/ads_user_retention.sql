-- 用户留存率

drop table if exists `ads_user_retention`;

create table `ads_user_retention`
(
    `dt`              date                                                   NOT NULL COMMENT '统计日期',
    `create_date`     varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '用户新增日期',
    `retention_day`   int(20)                                                NOT NULL COMMENT '截至当前日期留存天数',
    `retention_count` bigint(20)                                             NULL DEFAULT NULL COMMENT '留存用户数量',
    `new_user_count`  bigint(20)                                             NULL DEFAULT NULL COMMENT '新增用户数量',
    `retention_rate`  decimal(16, 2)                                         NULL DEFAULT NULL COMMENT '留存率',
    PRIMARY KEY (`dt`, `create_date`, `retention_day`) USING BTREE
) engine = InnoDB
  character set = utf8
  collate = utf8_general_ci
    comment = '留存率'
  row_format = Dynamic;