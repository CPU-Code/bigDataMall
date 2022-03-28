-- 创建 用户注册事务事实表

drop table if exists dwd_user_register_inc;


create external table dwd_user_register_inc
(
    `user_id`        STRING COMMENT '用户ID',
    `date_id`        STRING COMMENT '日期ID',
    `create_time`    STRING COMMENT '注册时间',
    `channel`        STRING COMMENT '应用下载渠道',
    `province_id`    STRING COMMENT '省份id',
    `version_code`   STRING COMMENT '应用版本',
    `mid_id`         STRING COMMENT '设备id',
    `brand`          STRING COMMENT '设备品牌',
    `model`          STRING COMMENT '设备型号',
    `operate_system` STRING COMMENT '设备操作系统'
) comment '用户域用户注册事务事实表'
    partitioned by (`dt` string)
    stored as orc
    location '/warehouse/gmall/dwd/dwd_user_register_inc/'
    tblproperties ('orc.compress' = 'snappy')


-- 首日装载 用户注册事务事实表

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_user_register_inc
    partition (dt)
select ui.user_id,
       date_format(create_time, 'yyyy-MM-dd') date_id,
       create_time,
       channel,
       province_id,
       version_code,
       mid_id,
       brand,
       model,
       operate_system,
       date_format(create_time, 'yyyy-MM-dd') dt
from (
         select data.id user_id,
                data.create_time
         from ods_user_info_inc
         where dt = '2020-06-14'
           and type = 'bootstrap-insert'
     ) ui
         left join (
    select common.ar  area_code,
           common.ba  brand,
           common.ch  channel,
           common.md  model,
           common.mid mid_id,
           common.os  operate_system,
           common.uid user_id,
           common.vc  version_code
    from ods_log_inc
    where dt = '2020-06-14'
      and page.page_id = 'register'
      and common.uid is not null
) log
                   on ui.user_id = log.user_id
         left join(
    select id province_id,
           area_code
    from ods_base_province_full
    where dt = '2020-06-14'
) bp
                  on log.area_code = bp.area_code;


-- 每日装载 用户注册事务事实表

insert overwrite table dwd_user_register_inc
    partition (dt = '2020-06-15')
select ui.user_id,
       date_format(create_time, 'yyyy-MM-dd') date_id,
       create_time,
       channel,
       province_id,
       version_code,
       mid_id,
       brand,
       model,
       operate_system
from (
         select data.id user_id,
                data.create_time
         from ods_user_info_inc
         where dt = '2020-06-15'
           and type = 'insert'
     ) ui
         left join (
    select common.ar  area_code,
           common.ba  brand,
           common.ch  channel,
           common.md  model,
           common.mid mid_id,
           common.os  operate_system,
           common.uid user_id,
           common.vc  version_code
    from ods_log_inc
    where dt = '2020-06-15'
      and page.page_id = 'register'
      and common.uid is not null
) log
                   on ui.user_id = log.user_id
         left join(
    select id province_id,
           area_code
    from ods_base_province_full
    where dt = '2020-06-15'
) bp
                  on log.area_code = bp.area_code;


-- 查看分区

show partitions dwd_user_register_inc;


-- 查询 用户注册事务事实表

select user_id,
       date_id,
       create_time,
       channel,
       province_id,
       version_code,
       mid_id,
       brand,
       model,
       operate_system,
       dt
from dwd_user_register_inc
where dt = '2020-06-14'







