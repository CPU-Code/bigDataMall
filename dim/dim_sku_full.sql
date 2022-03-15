-- 删除 商品维度表

DROP TABLE IF EXISTS dim_sku_full;


-- 创建 商品维度表

CREATE EXTERNAL TABLE dim_sku_full
(
    `id`    STRING COMMENT 'sku_id',
    `price` DECIMAL(16, 2) COMMENT '商品价格'
) COMMENT '商品维度表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dim/dim_sku_full/'
    TBLPROPERTIES ('orc.compress' = 'snappy');




-- 装载 商品维度表













-- 查询 商品维度表

select id,
       price,
       dt
from dim_sku_full
where dt = '2020-06-14';


