-- 产品分类表
drop table if exists `ods_trade_product_category`;

create external table ods_trade_product_category
(
    `cat_id`      int comment '品类ID',
    `parent_id`   int comment '父ID',
    `cat_name`    string comment '分类名称',
    `is_show`     tinyint comment '是否显示0:隐藏 1:显示',
    `sort_num`    int comment '排序号',
    `is_del`      tinyint comment '删除标志 1:有效 -1：删除',
    `create_time` string comment '建立时间',
    `level`       tinyint comment '分类级别，共3级'
) comment '产品分类表'
    partitioned by (`dt` string)
    row format delimited fields terminated by ','
    location '/origin_data/lg_mall/trade.db/product_category/';

-- 装载数据
set hivevar:do_date = 2020-07-12;
alter table ods_trade_product_category
    add partition (dt = '${do_date}');


-- 查询数据
select cat_id,
       parent_id,
       cat_name,
       is_show,
       sort_num,
       is_del,
       create_time,
       level
from ods_trade_product_category
where dt = '${do_date}'
limit 10;


