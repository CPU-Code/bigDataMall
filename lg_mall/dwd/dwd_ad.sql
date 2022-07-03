-- 广告点击明细
drop table if exists dwd_ad;

create external table dwd_ad
(
    `device_id`   string comment '设备id',
    `uid`         string comment '用户id',
    `app_v`       string comment 'app版本',
    `os_type`     string comment '系统版本',
    `event_type`  string comment '事件类型',
    `language`    string comment '语言',
    `channel`     string comment '渠道',
    `area`        string comment '地位',
    `brand`       string comment '品牌',
    `report_time` string comment '事件时间',
    `duration`    int comment '停留时长',
    `ad_action`   int comment '用户行为: 0 曝光 1 曝光后点击 2 购买',
    `shop_id`     int comment '商家id',
    `ad_type`     int comment '格式类型: 1 JPG 2 PNG 3 GIF 4 SWF',
    `show_style`  smallint comment '显示风格: 0 静态图 1 动态图',
    `product_id`  int comment '产品id',
    `place`       string comment '广告位置: 首页 1 左侧 2 右侧 3 列表页 4',
    `sort`        int comment '排序位置',
    `hour`        string comment '小时'
) comment '广告点击明细'
    partitioned by (`dt` string)
    stored as parquet;


-- 装载数据
set hive.execution.engine = spark;
set hivevar:do_date = 2020-07-21;
insert overwrite table dwd_ad
    partition (dt = '${do_date}')
select device_id,
       uid,
       app_v,
       os_type,
       event_type,
       language,
       channel,
       area,
       brand,
       report_time,
       get_json_object(event_json, '$.duration'),
       get_json_object(event_json, '$.ad_action'),
       get_json_object(event_json, '$.shop_id'),
       get_json_object(event_json, '$.ad_type'),
       get_json_object(event_json, '$.show_style'),
       get_json_object(event_json, '$.product_id'),
       get_json_object(event_json, '$.place'),
       get_json_object(event_json, '$.sort'),
       from_unixtime(ceil(report_time / 1000), 'HH')
from dwd_event_log
where dt = '${do_date}'
  and name = 'ad';


-- 查询数据
set hivevar:do_date = 2020-07-21;
select device_id,
       uid,
       app_v,
       os_type,
       event_type,
       language,
       channel,
       area,
       brand,
       report_time,
       duration,
       ad_action,
       shop_id,
       ad_type,
       show_style,
       product_id,
       place,
       sort,
       hour,
       dt
from dwd_ad
where dt = '${do_date}'
limit 10;