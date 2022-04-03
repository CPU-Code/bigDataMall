-- 创建各活动补贴率

drop table if exists `ads_activity_stats`;

create table `ads_activity_stats`
(
    `dt`            date                                                   NOT NULL COMMENT '统计日期',
    `activity_id`   varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '活动ID',
    `activity_name` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '活动名称',
    `start_date`    varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '活动开始日期',
    `reduce_rate`   decimal(16, 2)                                         NULL DEFAULT NULL COMMENT '补贴率',
    PRIMARY KEY (`dt`, `activity_id`) USING BTREE
) engine = InnoDB
  character set = utf8
  collate = utf8_general_ci comment '活动统计'
  row_format = Dynamic;