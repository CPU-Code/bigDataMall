# bigDataMall

电商数仓

CSDN : [https://blog.csdn.net/qq_44226094/category_11631933.html?spm=1001.2014.3001.5482](https://blog.csdn.net/qq_44226094/category_11631933.html?spm=1001.2014.3001.5482)


集群服务器规划 :


| 服务名称        | 子服务              | 服务器 cpu101| 服务器 cpu102| 服务器 cpu103 |
| :------------: | :-----------: | :-------------: | :-------------: | :--------: |
| HDFS              | NameNode         | √             |      √        |     √       |
|           |     DataNode           | √             | √             |          √      |
|  |    JournalNode          |      √            |     √        |       √           |
|  |   ZKFC          |         √         |    √         |       √           |
|  |       |               |        |               |
| Yarn              | NodeManager      | √             | √             | √             |
|   |     Resourcemanager              |     √    |         √      |       √    |
|  |       |               |        |               |
| Zookeeper         | Zookeeper Server | √             | √             | √             |
|  |       |               |        |               |
| Flume     | 采集日志            | √             | √             |               |
|    | 消费Kafka          |              |              | √     |
|  |       |               |        |               |
| Kafka             | Kafka            | √             | √             | √            |
| Hive              | Hive             | √             |               |               |
| MySQL             | MySQL            |             |         √      |               |
| DataX             | DataX            | √             |               |               |
| Maxwell           | Maxwell          | √             |               |               |
|  |       |               |        |               |
| Presto            | Coordinator      | √             |               |               |
|  | Worker            | √                | √             | √             |           
|  |       |               |        |               |
| DolphinScheduler  | MasterServer     | √             |               |               |
 |  | WorkerServer      | √                | √             | √             |   
 |  |       |               |        |               |
| Druid             | Druid            | √             | √             | √             |
| Kylin             |                  | √             |               |               |
|  |       |               |        |               |
| Hbase             | HMaster          | √             |       √     |      √     |
|  | HRegionServer     | √                | √             | √             |   
|  |       |               |        |               |
| Superset          |                  | √             |               |               |
| Atlas             |                  | √             |               |               |
| Solr              | Jar              | √             | √             | √             |






---------------------

数据来源 : 

* 行为日志数据
* 业务数据



行为日志数据 : 

* 页面浏览记录 
* 动作记录
* 曝光记录
* 启动记录
* 错误记录



业务数据 : 

全量 :

* activity_info 活动表 
* activity_rule 优惠规则表
* base_category1 商品一级分类
* base_category2 商品二级分类
* base_category3 商品三级分类
* base_dic 编码字典表
* base_province 省份表
* base_region 地区表
* base_trademark 品牌表
* cart_info 加购表(特殊)
* coupon_info 优惠卷表
* sku_attr_value SKU平台属性表
* sku_sale_attr_value SKU销售属性表
* sku_info SKU商品表
* spu_info SPU商品表

增量 :

* cart_info 加购表 ( 特殊 )
* comment_info 商品评论表
* coupon_use 优惠卷领用表
* favor_info 收藏表
* order_detail_activity 订单明细活动关联表
* order_detail_coupon 订单明细优惠卷关联表
* order_detail 订单详情表
* order_info 订单表
* order_refund_info 退单表
* order_status_log 订单状态表
* payment_info 支付表
* refund_payment 退款表
* user_info 用户表

------------------------------


技术栈 : 

* Flume : 日志收集
* Kafka : 实时缓存
* DataX : 全量同步
* Maxwell : 增量同步
* Hive : 查询
* Spark : 计算引擎
* HDFS : 存储
* YARN : 资源调度
* Zookeeper : HA选举
* Mysql : 元数据存储
* DolphinScheduler : 任务调度
* Zabbix : 集群监控
* Atlas : 元数据管理
* Superset : 数据可视化
* JDK8 : Java环境
* Python : 脚本
* Shell : 脚本

------------------------------



仓库分层规划 : 

* ods : 原始数据层
* dim : 公共维度层
* dwd : 明细数据层
* dws : 汇总数据层
* ads : 数据应用层



----------------

# [1 原始数据层](ods)


## [1.1 日志表](ods/log)

- [x] [日志表](ods/log/ods_log_inc.sql)


- [] [日志表每日数据装载](ods/hdfs_to_ods_log.sh)

## [1.2 全量表](ods/full)

- [x] [活动信息表](ods/full/ods_activity_info_full.sql)
- [ ] [活动规则表](ods/full/ods_activity_rule_full.sql)
- [ ] [一级品类表](ods/full/ods_base_category1_full.sql)
- [ ] [二级品类表](ods/full/ods_base_category2_full.sql)
- [ ] [三级品类表](ods/full/ods_base_category3_full.sql)
- [ ] [编码字典表](ods/full/ods_base_dic_full.sql)
- [ ] [省份表](ods/full/ods_base_province_full.sql)
- [ ] [地区表](ods/full/ods_base_region_full.sql)
- [ ] [品牌表](ods/full/ods_base_trademark_full.sql)
- [ ] [购物车表](ods/full/ods_cart_info_full.sql)
- [ ] [优惠券信息表](ods/full/ods_coupon_info_full.sql)
- [ ] [商品平台属性表](ods/full/ods_sku_attr_value_full.sql)
- [ ] [商品表](ods/full/ods_sku_info_full.sql)
- [ ] [商品销售属性值表](ods/full/ods_sku_sale_attr_value_full.sql)
- [ ] [SPU表](ods/full/ods_spu_info_full.sql)


## [1.3 增量表](ods/inc)

- [ ] [购物车表](ods/inc/ods_cart_info_inc.sql)
- [ ] [评论表](ods/inc/ods_comment_info_inc.sql)
- [ ] [优惠券领用表](ods/inc/ods_coupon_use_inc.sql)
- [ ] [收藏表](ods/inc/ods_favor_info_inc.sql)
- [ ] [订单明细表](ods/inc/ods_order_detail_inc.sql)
- [ ] [订单明细活动关联表](ods/inc/ods_order_detail_activity_inc.sql)
- [ ] [订单明细优惠券关联表](ods/inc/ods_order_detail_coupon_inc.sql)
- [ ] [订单表](ods/inc/ods_order_info_inc.sql)
- [ ] [退单表](ods/inc/ods_order_refund_info_inc.sql)
- [ ] [订单状态流水表](ods/inc/ods_order_status_log_inc.sql)
- [ ] [支付表](ods/inc/ods_payment_info_inc.sql)
- [ ] [退款表](ods/inc/ods_refund_payment_inc.sql)
- [ ] [用户表](ods/inc/ods_user_info_inc.sql)



----------------

# [2 公共维度层](dim)


- [x] [商品维度表](dim/dim_sku_full.sql)
- [x] [优惠券维度表](dim/dim_coupon_full.sql)
- [x] [活动维度表](dim/dim_activity_full.sql)
- [x] [地区维度表](dim/dim_province_full.sql)
- [x] [日期维度表](dim/dim_date.sql)
- [ ] [用户维度表](dim/dim_user_zip.sql)

----------------

# [3 明细数据层](dwd)

## [交易域](dwd/trade)

- [x] [加购事务事实表](dwd/trade/dwd_trade_cart_add_inc.sql)
- [x] [下单事务事实表](dwd/trade/dwd_trade_order_detail_inc.sql)
- [x] [取消订单事务事实表](dwd/trade/dwd_trade_cancel_detail_inc.sql)
- [x] [支付成功事务事实表](dwd/trade/dwd_trade_pay_detail_suc_inc.sql)
- [x] [退单事务事实表](dwd/trade/dwd_trade_order_refund_inc.sql)
- [x] [退款成功事务事实表](dwd/trade/dwd_trade_refund_pay_suc_inc.sql)
- [x] [购物车周期快照事实表](dwd/trade/dwd_trade_cart_full.sql)


## [工具域](dwd/tool)

- [x] [优惠券领取事务事实表](dwd/tool/dwd_tool_coupon_get_inc.sql)
- [x] [优惠券使用下单事务事实表](dwd/tool/dwd_tool_coupon_order_inc.sql)
- [x] [优惠券使用支付事务事实表](dwd/tool/dwd_tool_coupon_pay_inc.sql)

## [互动域](dwd/interaction)

- [x] [收藏商品事务事实表](dwd/interaction/dwd_interaction_favor_add_inc.sql)
- [x] [评价事务事实表](dwd/interaction/dwd_interaction_comment_inc.sql)


## [流量域](dwd/traffic)

- [x] [页面浏览事务事实表](dwd/traffic/dwd_traffic_page_view_inc.sql)
- [x] [启动事务事实表](dwd/traffic/dwd_traffic_start_inc.sql)
- [ ] [动作事务事实表](dwd/traffic/dwd_traffic_action_inc.sql)
- [ ] [曝光事务事实表](dwd/traffic/dwd_traffic_display_inc.sql)
- [ ] [错误事务事实表](dwd/traffic/dwd_traffic_error_inc.sql)


## [用户域](dwd/user)

- [ ] [用户注册事务事实表](dwd/user/dwd_user_register_inc.sql)
- [ ] [用户登录事务事实表](dwd/user/dwd_user_login_inc.sql)


----------------

# [4 汇总数据层](dws)


## [最近1日汇总表](dws/1d)


- [ ] [交易域用户商品粒度订单最近1日汇总表](dws/1d/trade/dws_trade_user_sku_order_1d.sql)
- [ ] [交易域用户商品粒度退单最近1日汇总表](dws/1d/trade/dws_trade_user_sku_order_refund_1d.sql)
- [ ] [交易域用户粒度订单最近1日汇总表](dws/1d/trade/dws_trade_user_order_1d.sql)
- [ ] [交易域用户粒度加购最近1日汇总表](dws/1d/trade/dws_trade_user_cart_add_1d.sql)
- [ ] [交易域用户粒度支付最近1日汇总表](dws/1d/trade/dws_trade_user_payment_1d.sql)
- [ ] [交易域用户粒度退单最近1日汇总表](dws/1d/trade/dws_trade_user_order_refund_1d.sql)
- [ ] [交易域省份粒度订单最近1日汇总表](dws/1d/trade/dws_trade_province_order_1d.sql)


- [ ] [流量域会话粒度页面浏览最近1日汇总表](dws/1d/traffic/dws_traffic_session_page_view_1d.sql)
- [ ] [流量域访客页面粒度页面浏览最近1日汇总表](dws/1d/traffic/dws_traffic_page_visitor_page_view_1d.sql)


- [ ] [最近1日汇总表数据装载脚本](dws/1d/dwd_to_dws_1d_init.sh)



## [最近n日汇总表](dws/nd)


- [ ] [交易域用户商品粒度订单最近n日汇总表](dws/nd/trade/dws_trade_user_sku_order_nd.sql)
- [ ] [交易域用户商品粒度退单最近n日汇总表](dws/nd/trade/dws_trade_user_sku_order_refund_nd.sql)
- [ ] [交易域用户粒度订单最近n日汇总表](dws/nd/trade/dws_trade_user_order_nd.sql)
- [ ] [交易域用户粒度加购最近n日汇总表](dws/nd/trade/dws_trade_user_cart_add_nd.sql)
- [ ] [交易域用户粒度支付最近n日汇总表](dws/nd/trade/dws_trade_user_payment_nd.sql)
- [ ] [交易域用户粒度退单最近n日汇总表](dws/nd/trade/dws_trade_user_order_refund_nd.sql)
- [ ] [交易域省份粒度订单最近n日汇总表](dws/nd/trade/dws_trade_province_order_nd.sql)
- [ ] [交易域优惠券粒度订单最近n日汇总表](dws/nd/trade/dws_trade_coupon_order_nd.sql)
- [ ] [交易域活动粒度订单最近n日汇总表](dws/nd/trade/dws_trade_activity_order_nd.sql)


- [ ] [流量域访客页面粒度页面浏览最近n日汇总表](dws/nd/traffic/dws_traffic_page_visitor_page_view_nd.sql)


- [ ] [最近n日汇总表数据装载脚本](dws/nd/dws_1d_to_dws_nd.sh)


## [历史至今汇总表](dws/td)

- [ ] [交易域用户粒度订单历史至今汇总表](dws/td/trade/dws_trade_user_order_td.sql)
- [ ] [交易域用户粒度支付历史至今汇总表](dws/td/trade/dws_trade_user_payment_td.sql)


- [ ] [用户域用户粒度登录历史至今汇总表](dws/td/user/dws_user_user_login_td.sql)


- [ ] [历史至今汇总表数据装载脚本](dws/td/dws_1d_to_dws_td_init.sh)


----------------

# [5 数据应用层](ads)



## [流量主题](ads/traffic)


- [ ] [各渠道流量统计](ads/traffic/ads_traffic_stats_by_channel.sql)
- [ ] [路径分析](ads/traffic/ads_page_path.sql)



## [用户主题](ads/user)


- [ ] [用户变动统计](ads/user/ads_user_change.sql)
- [ ] [用户留存率](ads/user/ads_user_retention.sql)
- [ ] [用户新增活跃统计](ads/user/ads_user_stats.sql)
- [ ] [用户行为漏斗分析](ads/user/ads_user_action.sql)
- [ ] [新增交易用户统计](ads/user/ads_new_buyer_stats.sql)



## [商品主题](ads/sku)


- [ ] [最近7/30日各品牌复购率](ads/sku/ads_repeat_purchase_by_tm.sql)
- [ ] [各品牌商品交易统计](ads/sku/ads_trade_stats_by_tm.sql)
- [ ] [各品类商品交易统计](ads/sku/ads_trade_stats_by_cate.sql)
- [ ] [各分类商品购物车存量Top10](ads/sku/ads_sku_cart_num_top3_by_cate.sql)



## [交易主题](ads/trade)


- [ ] [交易综合统计](ads/trade/ads_trade_stats.sql)
- [ ] [各省份交易统计](ads/trade/ads_order_by_province.sql)



## [优惠劵主题](ads/coupon)

- [ ] [最近30天发布的优惠券的补贴率](ads/coupon/ads_coupon_stats.sql)



## [活动主题](ads/activity)

- [ ] [最近30天发布的活动的补贴率](ads/activity/ads_activity_stats.sql)









