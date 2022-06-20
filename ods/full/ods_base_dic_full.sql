-- 删除 编码字典表
drop table if exists ods_base_dic_full;

-- 创建 编码字典表
create external table ods_base_dic_full
(
    `dic_code`     string comment '编号',
    `dic_name`     string comment '编码名称',
    `parent_code`  string comment '父编号',
    `create_time`  string comment '创建日期',
    `operate_time` string comment '修改日期'
) comment '编码字典表'
    partitioned by (`dt` string)
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/warehouse/gmall/ods/ods_base_dic_full/';


-- 装载数据
load data inpath '/origin_data/gmall/db/base_dic_full/2020-06-14'
    into table ods_base_dic_full
    partition (dt = '2020-06-14');


-- 查询数据
select dic_code,
       dic_name,
       parent_code,
       create_time,
       operate_time,
       dt
from ods_base_dic_full
where dt = '2020-06-14';








