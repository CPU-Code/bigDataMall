-- 删除 用户日启动汇总
drop table if exists dws_member_start_day;

-- 创建 用户日启动汇总
create table dws_member_start_day
(
    `device_id` string comment '设备id',
    `uid`       string comment '用户id',
    `app_v`     string comment 'app版本',
    `os_type`   string comment '系统版本',
    `language`  string comment '语言',
    `channel`   string comment '渠道',
    `area`      string comment '地区',
    `brand`     string comment '品牌'
) comment '用户日启动汇总'
    partitioned by (`dt` string)
    stored as parquet;

-- 装载数据
set hive.execution.engine = spark;
insert overwrite table dws_member_start_day
    partition (dt = '2020-07-22')
select device_id,
       concat_ws('|', collect_set(uid)),
       concat_ws('|', collect_set(app_v)),
       concat_ws('|', collect_set(os_type)),
       concat_ws('|', collect_set(language)),
       concat_ws('|', collect_set(channel)),
       concat_ws('|', collect_set(area)),
       concat_ws('|', collect_set(brand))
from dwd_start_log
where dt = '2020-07-22'
group by device_id;

-- 查询数据
select device_id,
       uid,
       app_v,
       os_type,
       language,
       channel,
       area,
       brand,
       dt
from dws_member_start_day
where dt = '2020-07-22';

