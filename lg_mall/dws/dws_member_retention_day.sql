-- 用户留存明细
drop table if exists dws_member_retention_day;

create table dws_member_retention_day
(
    `device_id`      string comment '设备id',
    `uid`            string comment '用户id',
    `app_v`          string comment 'app版本',
    `os_type`        string comment '系统版本',
    `language`       string comment '语言',
    `channel`        string comment '渠道',
    `area`           string comment '地区',
    `brand`          string comment '品牌',
    `add_date`       string comment '用户新增时间',
    `retention_date` int comment '留存天数'
) comment '每日用户留存明细'
    partitioned by (`dt` string)
    stored as parquet;

-----------------------

-- 装载数据
set hivevar:do_date= 2020-07-22;
select t1.device_id,
       t1.uid,
       t1.app_v,
       t1.os_type,
       t1.language,
       t1.channel,
       t1.area,
       t1.brand,
       t1.dt add_date,
       1
from dws_member_start_day t1
         join dws_member_add_day t2
              on
                  t1.device_id = t2.device_id
where t1.dt = '${do_date}'
  and t2.dt = date_add('${do_date}', -1);


set hivevar:do_date= 2020-07-22;
insert overwrite table dws_member_retention_day
    partition (dt = '${do_date}')
    (
        select t1.device_id,
               t1.uid,
               t1.app_v,
               t1.os_type,
               t1.language,
               t1.channel,
               t1.area,
               t1.brand,
               t1.dt add_date,
               1
        from dws_member_start_day t1
                 join dws_member_add_day t2
                      on t1.device_id = t2.device_id
        where t1.dt = '${do_date}'
          and t2.dt = date_add('${do_date}', -1)
        union all
        select t1.device_id,
               t1.uid,
               t1.app_v,
               t1.os_type,
               t1.language,
               t1.channel,
               t1.area,
               t1.brand,
               t1.dt add_date,
               2
        from dws_member_start_day t1
                 join dws_member_add_day t2
                      on t1.device_id = t2.device_id
        where t2.dt = date_add('${do_date}', -2)
          and t1.dt = '${do_date}'
        union all
        select t1.device_id,
               t1.uid,
               t1.app_v,
               t1.os_type,
               t1.language,
               t1.channel,
               t1.area,
               t1.brand,
               t1.dt add_date,
               3
        from dws_member_start_day t1
                 join dws_member_add_day t2
                      on t1.device_id = t2.device_id
        where t2.dt = date_add('${do_date}', -3)
          and t1.dt = '${do_date}'
    );


-- 查询数据
select device_id,
       uid,
       app_v,
       os_type,
       language,
       channel,
       area,
       brand,
       add_date,
       retention_date,
       dt
from dws_member_retention_day
where dt = '${do_date}';
