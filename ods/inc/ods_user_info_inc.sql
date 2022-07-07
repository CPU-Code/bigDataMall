-- 创建用户表
drop table if exists ods_user_info_inc;

create external table ods_user_info_inc
(
    `type` string comment '变动类型',
    `ts`   bigint comment '变动时间',
    `data` struct<id :string,login_name :string,nick_name :string,passwd :string,name :string,phone_num :string,email
                  :string,head_img :string,user_level :string,birthday :string,gender :string,create_time :string,operate_time
                  :string,status :string> comment '数据',
    `old`  map<string,string> comment '旧值'
) comment '用户表'
    partitioned by (`dt` string)
    row format serde 'org.apache.hadoop.hive.serde2.JsonSerDe'
    location '/warehouse/gmall/ods/ods_user_info_inc/';


-- 装载数据
load data inpath '/origin_data/gmall/db/user_info_inc/2020-06-14'
    into table ods_user_info_inc
    partition (dt = '2020-06-14');

-- 查询数据
select type,
       ts,
       data.id,
       data.login_name,
       data.nick_name,
       data.passwd,
       data.name,
       data.phone_num,
       data.email,
       data.head_img,
       data.user_level,
       data.birthday,
       data.gender,
       data.create_time,
       data.operate_time,
       data.status,
       old,
       dt
from ods_user_info_inc
where dt = '2020-06-14'
limit 10;


