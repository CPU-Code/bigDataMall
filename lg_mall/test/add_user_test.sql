-- 日启动表
drop table start_day_test;

-- 创建日启动表
create table start_day_test
(
    id int,
    dt string
)
    row format delimited fields terminated by ',';


-- 装载数据
load data inpath '/origin_data/cpucode/test/t10.txt'
    into table start_day_test;

-- 查询数据
select id,
       dt
from start_day_test;

--------------------------------------------

-- 创建全量数据

drop table start_all_test;

create table start_all_test
(
    id int,
    dt string
) row format delimited fields terminated by ',';


-- 装载数据
load data inpath '/origin_data/cpucode/test/test11.txt'
    into table start_all_test;


-- 查询数据
select id,
       dt
from start_all_test;

--------------------------------------------

-- 找出 2020-08-02 的新用户

-- 日用户 left join 全用户
select t1.id,
       t1.dt,
       t2.id,
       t2.dt
from start_day_test t1
         left join start_all_test t2
                   on t1.id = t2.id
where t1.dt = '2020-08-02';

-- 排除非新用户
select t1.id,
       t1.dt
from start_day_test t1
         left join start_all_test t2
                   on t1.id = t2.id
where t1.dt = "2020-08-02"
  and t2.id is null;

--------------------------------------------

-- 将 2020-08-02 新用户插入全量表

insert into table start_all_test
select t1.id,
       t1.dt
from start_day_test t1
         left join start_all_test t2
                   on t1.id = t2.id
where t1.dt = "2020-08-02"
  and t2.id is null;


-- 检查结果
select *
from start_all_test;


--------------------------

-- 装载2020-08-03数据

load data inpath '/origin_data/cpucode/test/test3.txt'
    into table start_day_test;

-- 查询数据
select *
from start_day_test;

-- 装载新用户
insert into table start_all_test
select t1.id,
    t1.dt
from start_day_test t1
         left join start_all_test t2
                   on t1.id = t2.id
where t1.dt = '2020-08-03'
  and t2.id is null;

-- 查询所有用户
select *
from start_all_test;