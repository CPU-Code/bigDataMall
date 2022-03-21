-- 创建 收藏商品事务事实表

drop table if exists dwd_interaction_favor_add_inc;

create external table dwd_interaction_favor_add_inc
(
    `id`          STRING COMMENT '编号',
    `user_id`     STRING COMMENT '用户id',
    `sku_id`      STRING COMMENT 'sku_id',
    `date_id`     STRING COMMENT '日期id',
    `create_time` STRING COMMENT '收藏时间'
) comment '收藏商品事务事实表'
    partitioned by (`dt` string)
    stored as orc
    location '/warehouse/gmall/dwd/dwd_interaction_favor_add_inc/'
    tblproperties ('orc.compress' = 'snappy');


-- 首日装载 收藏商品事务事实表
set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table dwd_interaction_favor_add_inc
    partition (dt)
select data.id,
       data.user_id,
       data.sku_id,
       date_format(data.create_time, 'yyyy-MM-dd') date_id,
       data.create_time,
       date_format(data.create_time, 'yyyy-MM-dd') dt
from ods_favor_info_inc
where dt = '2020-06-14'
  and type = 'bootstrap-insert';


-- 每日装载 收藏商品事务事实表

insert overwrite table dwd_interaction_favor_add_inc
    partition (dt = '2020-06-15')
select data.id,
       data.user_id,
       data.sku_id,
       date_format(data.create_time, 'yyyy-MM-dd') date_id,
       data.create_time
from ods_favor_info_inc
where dt = '2020-06-15'
  and type = 'insert';


-- 查询分区

show partitions dwd_interaction_favor_add_inc;


-- 查询 收藏商品事务事实表


select id,
       user_id,
       sku_id,
       date_id,
       create_time,
       dt
from dwd_interaction_favor_add_inc
where dt = '2020-06-14';










