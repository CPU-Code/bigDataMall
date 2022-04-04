-- 各渠道流量统计

drop table if exists `ads_traffic_stats_by_channel`;

create table `ads_traffic_stats_by_channel`
(
    `dt`               date                                                   NOT NULL COMMENT '统计日期',
    `recent_days`      bigint(20)                                             NOT NULL COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `channel`          varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '渠道',
    `uv_count`         bigint(20)                                             NULL DEFAULT NULL COMMENT '访客人数',
    `avg_duration_sec` bigint(20)                                             NULL DEFAULT NULL COMMENT '会话平均停留时长，单位为秒',
    `avg_page_count`   bigint(20)                                             NULL DEFAULT NULL COMMENT '会话平均浏览页面数',
    `sv_count`         bigint(20)                                             NULL DEFAULT NULL COMMENT '会话数',
    `bounce_rate`      decimal(16, 2)                                         NULL DEFAULT NULL COMMENT '跳出率',
    PRIMARY KEY (`dt`, `recent_days`, `channel`) USING BTREE
) engine = InnoDB
  character set = utf8
  collate = utf8_general_ci comment = '各渠道流量统计'
  row_format = Dynamic;

