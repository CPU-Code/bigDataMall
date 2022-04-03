-- 创建 用户路径分析

drop table if exists `ads_page_path`;

create table `ads_page_path`
(
    `dt`          date                                                   NOT NULL COMMENT '统计日期',
    `recent_days` bigint(20)                                             NOT NULL COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `source`      varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '跳转起始页面ID',
    `target`      varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '跳转终到页面ID',
    `path_count`  bigint(20)                                             NULL DEFAULT NULL COMMENT '跳转次数',
    PRIMARY KEY (`dt`, `recent_days`, `source`, `target`) USING BTREE
) engine = InnoDB
  character set = utf8
  collate = utf8_general_ci
    comment = '页面浏览路径分析'
  row_format = Dynamic;





