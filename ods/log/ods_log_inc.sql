-- 删除日志表

DROP TABLE IF EXISTS ods_log_inc;


-- 创建外部日志表

CREATE EXTERNAL TABLE ods_log_inc
(
    `common`   STRUCT<ar :STRING, ba :STRING, ch :STRING, is_new :STRING, md :STRING, mid :STRING, os :STRING, uid
                      :STRING, vc :STRING> COMMENT '公共信息',
    `page`     STRUCT<during_time :BIGINT, item :STRING, item_type :STRING, last_page_id :STRING, page_id :STRING,
                      source_type :STRING> COMMENT '页面信息',
    `actions`  ARRAY<STRUCT<action_id:STRING, item:STRING, item_type:STRING, ts:BIGINT>> COMMENT '动作信息',
    `displays` ARRAY<STRUCT<display_type :STRING, item :STRING, item_type :STRING, `order` :STRING, pos_id
                            :STRING>> COMMENT '曝光信息',
    `start`    STRUCT<entry :STRING, loading_time :BIGINT, open_ad_id :BIGINT, open_ad_ms :BIGINT, open_ad_skip_ms
                      :BIGINT> COMMENT '启动信息',
    `err`      STRUCT<error_code:BIGINT, msg:STRING> COMMENT '错误信息',
    `ts`       BIGINT COMMENT '时间戳'
) COMMENT '活动信息表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/warehouse/gmall/ods/ods_log_inc/';


--- 装载数据

load data inpath '/origin_data/gmall/log/topic_log/2020-06-15'
    into table ods_log_inc
    partition (dt = '2020-06-15');


-- 查询数据
set hive.execution.engine = spark;
select common.ar         as area_code,
       common.ba         as brand,
       common.ch         as channel,
       common.is_new,
       common.md         as model,
       common.mid        as mid_id,
       common.os         as operate_system,
       common.uid        as user_id,
       common.vc         as version_code,
       page.during_time,
       page.item         as page_item,
       page.item_type    as page_item_type,
       page.last_page_id,
       page.page_id,
       page.source_type,
       action.action_id,
       action.item,
       action.item_type,
       action.ts,
       display.display_type,
       display.item      as display_item,
       display.item_type as display_item_type,
       display.`order`   as display_order,
       display.pos_id    as display_pos_id,
       action.action_id as action_id,
       `start`.entry,
       `start`.loading_time,
       `start`.open_ad_id ,
       `start`.open_ad_ms,
       `start`.open_ad_skip_ms,
       err.error_code,
       err.msg,
       ts,
       dt
from ods_log_inc lateral view explode(displays) tem as display
         lateral view explode(actions) tem as action
where dt = '2020-06-14'
  and actions is not null
limit 10;
