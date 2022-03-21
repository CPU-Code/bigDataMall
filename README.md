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
| Flume(采集日志)       | Flume            | √             | √             |               |
| Flume（消费Kafka）    | Flume          |              |              | √     |
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
* aps : 数据应用层



----------------

# [1 原始数据层](ods)


## [1.1 日志表](ods/log)

- [x] [日志表](ods/log/ods_log_inc.sql)


## [1.2 全量表](ods/full)

- [x] [活动信息表](ods/full/ods_activity_info_full.sql)


## [1.3 增量表](ods/inc)

- [ ] [购物车表](ods/inc/ods_cart_info_inc.sql)



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


## [互动域](dwd/interaction)




## [流量域](dwd/traffic)





## [用户域](dwd/user)







----------------

# [4 汇总数据层](dws)






----------------

# [5 数据应用层](ads)




