-- 删除评论表
drop table if exists ods_comment_info_inc;

-- 创建 评论表
create external table ods_comment_info_inc
(
    `type` string comment '变动类型',
    `ts`   bigint comment '变动时间',
    `data` struct<id :string, user_id :string, nick_name :string, head_img :string, sku_id :string,spu_id :string,
                  order_id :string, appraise :string, comment_txt :string, create_time :string, operate_time
                  :string> comment '数据',
    `old`  map<string,string> comment '旧值'
) comment ''
    partitioned by (`dt` string)
    row format serde 'org.apache.hadoop.hive.serde2.JsonSerDe'
    location '/warehouse/gmall/ods/ods_comment_info_inc/';

-- 装载数据
load data inpath '/origin_data/gmall/db/comment_info_inc/2020-06-14'
    into table ods_comment_info_inc
    partition (dt = '2020-06-14');

-- 查询数据
select type,
       ts,
       data.id,
       data.user_id,
       data.nick_name,
       data.head_img,
       data.sku_id,
       data.spu_id,
       data.order_id,
       data.appraise,
       data.comment_txt,
       data.create_time,
       data.operate_time
       old,
       dt
from ods_comment_info_inc
where dt = '2020-06-14';