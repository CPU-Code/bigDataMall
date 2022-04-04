-- 各品类商品购物车存量topN

drop table if exists `ads_sku_cart_num_top3_by_cate`;

create table `ads_sku_cart_num_top3_by_cate`
(
    `dt`             date                                                    NOT NULL COMMENT '统计日期',
    `category1_id`   varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci  NOT NULL COMMENT '一级分类ID',
    `category1_name` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci  NULL DEFAULT NULL COMMENT '一级分类名称',
    `category2_id`   varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci  NOT NULL COMMENT '二级分类ID',
    `category2_name` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci  NULL DEFAULT NULL COMMENT '二级分类名称',
    `category3_id`   varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci  NOT NULL COMMENT '三级分类ID',
    `category3_name` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci  NULL DEFAULT NULL COMMENT '三级分类名称',
    `sku_id`         varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci  NOT NULL COMMENT '商品id',
    `sku_name`       varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '商品名称',
    `cart_num`       bigint(20)                                              NULL DEFAULT NULL COMMENT '购物车中商品数量',
    `rk`             bigint(20)                                              NULL DEFAULT NULL COMMENT '排名',
    PRIMARY KEY (`dt`, `sku_id`, `category1_id`, `category2_id`, `category3_id`) USING BTREE
) engine = InnoDB
  character set = utf8
  collate = utf8_general_ci comment = '各分类商品购物车存量Top10'
  row_format = Dynamic;
