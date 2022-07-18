-- 商家地域组织表
drop table if exists ods_trade_shop_admin_org;

create external table ods_trade_shop_admin_org
(
    `id`          int comment '组织ID',
    `parent_id`   int comment '父ID',
    `org_name`    string comment '组织名称',
    `org_level`   tinyint comment '组织级别: 1总部及大区级部门; 2总部下属的各个部门及基部门; 3具体工作部门',
    `is_delete`   tinyint comment '删除标志: 1删除; 0有效',
    `create_time` string comment '创建时间',
    `update_time` string comment '最后修改时间',
    `is_show`     tinyint comment '是否显示: 0是; 1否',
    `org_type`    tinyint comment '组织类型 0:总裁办; 1:研发; 2:销售; 3:运营; 4:产品'
) comment '商家地域组织表'
    partitioned by (`dt` string)
    row format delimited fields terminated by ','
    location '/origin_data/lg_mall/trade.db/shop_org';


-- 加载数据
set hivevar:do_date = 2020-07-12;
alter table ods_trade_shop_admin_org
    add partition (dt = '${do_date}');




-- 查询数据
set hivevar:do_date = 2020-07-12;
select id,
       parent_id,
       org_name,
       org_level,
       is_delete,
       create_time,
       update_time,
       is_show,
       org_type
from ods_trade_shop_admin_org
where dt = '${do_date}'
limit 10;