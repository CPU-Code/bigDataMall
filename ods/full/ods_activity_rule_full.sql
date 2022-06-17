-- 删除 活动规则表
DROP TABLE IF EXISTS ods_activity_rule_full;

-- 创建外部 活动规则表
CREATE EXTERNAL TABLE ods_activity_rule_full
(
    `id`               STRING COMMENT '编号',
    `activity_id`      STRING COMMENT '类型',
    `activity_type`    STRING COMMENT '活动类型',
    `condition_amount` DECIMAL(16, 2) COMMENT '满减金额',
    `condition_num`    BIGINT COMMENT '满减件数',
    `benefit_amount`   DECIMAL(16, 2) COMMENT '优惠金额',
    `benefit_discount` DECIMAL(16, 2) COMMENT '优惠折扣',
    `benefit_level`    STRING COMMENT '优惠级别'
) COMMENT '活动规则表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/gmall/ods/ods_activity_rule_full/';


-- 装载数据

load data inpath '/origin_data/gmall/db/activity_rule_full/2020-06-14'
    into table ods_activity_rule_full
    partition (dt = '2020-06-14');


-- 查询数据

select id,
       activity_id,
       activity_type,
       condition_amount,
       condition_num,
       benefit_amount,
       benefit_discount,
       benefit_level,
       dt
from ods_activity_rule_full
where dt = '2020-06-14';
