-- 用户新增活跃统计

drop table if exists `ads_user_stats`;

create table `ads_user_stats`
(
    `dt`                date       NOT NULL COMMENT '统计日期',
    `recent_days`       bigint(20) NOT NULL COMMENT '最近n日,1:最近1日,7:最近7日,30:最近30日',
    `new_user_count`    bigint(20) NULL DEFAULT NULL COMMENT '新增用户数',
    `active_user_count` bigint(20) NULL DEFAULT NULL COMMENT '活跃用户数',
    PRIMARY KEY (`dt`, `recent_days`) USING BTREE
) engine = InnoDB
  character set = utf8
  collate = utf8_general_ci comment = '用户新增活跃统计'
  row_format = Dynamic;