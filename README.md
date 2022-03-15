# bigDataMall


电商数仓

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
* JDK8 : Java环境
* Python : 脚本
* Shell : 脚本


分层 : 

* ods : 原始数据层
* dim 公共维度层
* dwd 明细数据层
* dws 汇总数据层
* aps 数据应用层



----------------

# [1 原始数据层](ods)


## [1.1 日志表](ods/log)

- [x] [日志表](ods/log/ods_log_inc.sql)


## [1.2 全量表](ods/full)

- [x] [活动信息表](ods/full/ods_activity_info_full.sql)




## [1.3 增量表](ods/inc)

- [ ] [](ods/full/)



----------------

# [公共维度层](dim)





----------------

# [明细数据层](dwd)







----------------

# [汇总数据层](dws)






----------------

# [数据应用层](aps)




