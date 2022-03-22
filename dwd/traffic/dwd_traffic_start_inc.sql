-- 创建 启动事务事实表

drop table if exists dwd_traffic_start_inc;

create external table dwd_traffic_start_inc
(
    `province_id`     STRING COMMENT '省份id',
    `brand`           STRING COMMENT '手机品牌',
    `channel`         STRING COMMENT '渠道',
    `is_new`          STRING COMMENT '是否首次启动',
    `model`           STRING COMMENT '手机型号',
    `mid_id`          STRING COMMENT '设备id',
    `operate_system`  STRING COMMENT '操作系统',
    `user_id`         STRING COMMENT '会员id',
    `version_code`    STRING COMMENT 'app版本号',
    `entry`           STRING COMMENT 'icon手机图标 notice 通知',
    `open_ad_id`      STRING COMMENT '广告页ID ',
    `date_id`         STRING COMMENT '日期id',
    `start_time`      STRING COMMENT '启动时间',
    `loading_time_ms` BIGINT COMMENT '启动加载时间',
    `open_ad_ms`      BIGINT COMMENT '广告总共播放时间',
    `open_ad_skip_ms` BIGINT COMMENT '用户跳过广告时点'
) comment '启动日志表'
    partitioned by (`dt` string)
    stored as orc
    location '/warehouse/gmall/dwd/dwd_traffic_start_inc'
    tblproperties ('orc.compress' = 'snappy');


-- 数据装载 启动事务事实表
set hive.cbo.enable=false;

insert overwrite table dwd_traffic_start_inc
    partition (dt = '2020-06-14')
select province_id,
       brand,
       channel,
       is_new,
       model,
       mid_id,
       operate_system,
       user_id,
       version_code,
       entry,
       open_ad_id,
       date_format(from_utc_timestamp(ts, 'GMT+8'), 'yyyy-MM-dd')          date_id,
       date_format(from_utc_timestamp(ts, 'GMT+8'), 'yyyy-MM-dd HH:mm:ss') action_time,
       loading_time,
       open_ad_ms,
       open_ad_skip_ms
from (
         select common.ar  area_code,
                common.ba  brand,
                common.ch  channel,
                common.is_new,
                common.md  model,
                common.mid mid_id,
                common.os  operate_system,
                common.uid user_id,
                common.vc  version_code,
                `start`.entry,
                `start`.loading_time,
                `start`.open_ad_id,
                `start`.open_ad_ms,
                `start`.open_ad_skip_ms,
                ts
         from ods_log_inc
         where dt = '2020-06-14'
           and `start` is not null
     ) log
         left join (
    select id province_id,
           area_code
    from ods_base_province_full
    where dt = '2020-06-14'
) bp
                   on log.area_code = bp.area_code;


-- 查看分区

show partitions dwd_traffic_start_inc;


-- 查看 启动事务事实表
select province_id,
       brand,
       channel,
       is_new,
       model,
       mid_id,
       operate_system,
       user_id,
       version_code,
       entry,
       open_ad_id,
       date_id,
       start_time,
       loading_time_ms,
       open_ad_ms,
       open_ad_skip_ms,
       dt
from dwd_traffic_start_inc
where dt = '2020-06-14'







