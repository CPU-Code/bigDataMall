-- 删除 购物车表
drop table if exists ods_cart_info_full;

-- 创建 购物车表
create external table ods_cart_info_full
(
    `id`           string comment '编号',
    `user_id`      string comment '用户id',
    `sku_id`       string comment 'sku_id',
    `cart_price`   decimal(16, 2) comment '放入购物车时价格',
    `sku_num`      bigint comment '数量',
    `img_url`      string comment '商品图片地址',
    `sku_name`     string comment 'sku名称 (冗余)',
    `is_checked`   string comment '是否被选中',
    `create_time`  string comment '创建时间',
    `operate_time` string comment '修改时间',
    `is_ordered`   string comment '是否已经下单',
    `order_time`   string comment '下单时间',
    `source_type`  string comment '来源类型',
    `source_id`    string comment '来源编号'
) comment '购物车全量表'
    partitioned by (`dt` string)
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/warehouse/gmall/ods/ods_cart_info_full/';


-- 装载数据
load data inpath '/origin_data/gmall/db/cart_info_full/2020-06-14'
    into table ods_cart_info_full
    partition (dt = '2020-06-14');

-- 查询数据
select id,
       user_id,
       sku_id,
       cart_price,
       sku_num,
       img_url,
       sku_name,
       is_checked,
       create_time,
       operate_time,
       is_ordered,
       order_time,
       source_type,
       source_id,
       dt
from ods_cart_info_full
where dt = '2020-06-14';

