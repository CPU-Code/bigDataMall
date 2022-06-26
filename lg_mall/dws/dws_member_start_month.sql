-- 删除 用户月启动汇总
drop table if exists dws_member_start_month;

-- 创建 用户月启动汇总
create table dws_member_start_month
(
    `device_id` string comment '设备id',
    `uid`       string comment '用户id',
    `app_v`     string comment 'app版本',
    `os_type`   string comment '系统版本',
    `language`  string comment '语言',
    `channel`   string comment '渠道',
    `area`      string comment '地区',
    `brand`     string comment '品牌',
    `month`     string comment '月份'
) comment '用户月启动汇总'
    partitioned by (`dt` string)
    stored as parquet;

-- 装载每月活跃会员
set hive.execution.engine = spark;
insert overwrite table dws_member_start_month
    partition (dt = '2020-07-21')
select device_id,
       concat_ws('|', collect_set(uid)),
       concat_ws('|', collect_set(app_v)),
       concat_ws('|', collect_set(os_type)),
       concat_ws('|', collect_set(language)),
       concat_ws('|', collect_set(channel)),
       concat_ws('|', collect_set(area)),
       concat_ws('|', collect_set(brand)),
       date_format('2020-07-21', 'yyyy-MM')
from dws_member_start_day
where dt >= date_format('2020-07-21', 'yyyy-MM-01')
  and dt <= '2020-07-21'
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
       month,
       dt
from dws_member_start_month
where dt = '2020-07-21';