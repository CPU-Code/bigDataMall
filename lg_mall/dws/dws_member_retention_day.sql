-- 删除用户留存明细
drop table if exists dws_member_add_day;

-- 创建 用户留存明细
create table dws_member_add_day
(
    `device_id` string comment '设备id',
    `uid`       string comment '用户id',
    `app_v`     string comment 'app版本',
    `os_type`   string comment '系统版本',
    `language`  string comment '语言',
    `channel`   string comment '渠道',
    `area`      string comment '地区',
    `brand`     string comment '品牌',
    `dt`        string
) comment '每日新增会员明细'
    stored as parquet;


-- 装载数据
insert into table dws_member_add_day
select t1.device_id,
       t1.uid,
       t1.app_v,
       t1.os_type,
       t1.language,
       t1.channel,
       t1.area,
       t1.brand,
       t1.dt
from dws_member_start_day t1
         left join
     dws_member_add_day t2
     on t1.device_id = t2.device_id
where t1.dt = '2020-07-21'
  and t2.device_id is null;


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
from dws_member_add_day
where dt = '2020-07-21';
