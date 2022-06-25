-- 创建json 处理测试表
create table if not exists jsont1
(
    `username` string,
    `age`      int,
    `sex`      string,
    `json`     string
) row format delimited fields terminated by ';';

-- 装载数据
load data inpath '/origin_data/cpucode/test/test1.json'
    overwrite into table jsont1;

-- 基本查询
select username,
       age,
       sex,
       json
from jsont1;

-- get 单层值
select username,
       age,
       sex,
       get_json_object(json, '$.id')           id,
       get_json_object(json, '$.ids')          ids,
       get_json_object(json, '$.total_number') num
from jsont1;


-- 获取数组值
select username,
       age,
       sex,
       get_json_object(json, "$.id")           id,
       get_json_object(json, "$.ids[0]")       ids0,
       get_json_object(json, "$.ids[1]")       ids1,
       get_json_object(json, "$.ids[2]")       ids2,
       get_json_object(json, "$.ids[3]")       ids3,
       get_json_object(json, '$.total_number') num
from jsont1;

-- 一次处理多个字段
select json_tuple(json, 'id', 'ids', 'total_number')
from jsont1;


-- 有语法错误
select username,
       age,
       sex,
       json_tuple(json, 'id', 'ids', 'total_number')
from jsont1;


-- 将 [101,102,103] 中的 [ ] 替换掉
select regexp_replace("[101,102,103]", "\\[|\\]", "");

-- 将字符串变为数组
select split(regexp_replace("[101,102,103]", "\\[|\\]", ""), ",");


select username,
       age,
       sex,
       id,
       ids,
       num
from jsont1 lateral view json_tuple(json, 'id', 'ids', 'total_number') t1
         as id, ids, num;

with tmp as (
    select username,
           age,
           sex,
           id,
           ids,
           num
    from jsont1 lateral view json_tuple(json, 'id', 'ids', 'total_number') t1
             as id, ids, num
)
select username,
       age,
       sex,
       id,
       ids1,
       num
from tmp lateral view explode(split(regexp_replace(ids, "\\[|\\]", ""), ",")) t1 as ids1;