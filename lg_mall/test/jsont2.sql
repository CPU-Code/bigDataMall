create table t11
(
    id string
)
    stored as parquet;

create table t12
(
    id string
)
    stored as ORC;


desc formatted t11;

desc formatted t12;


create table jsont2
(
    id           int,
    ids          array<string>,
    total_number int
) row format serde 'org.apache.hive.hcatalog.data.JsonSerDe';


load data inpath '/origin_data/cpucode/test/test2.json'
    into table jsont2;


select id,
       ids,
       total_number
from jsont2;


