# ad_offline_dataware
这个项目是关于广告投放的数仓项目，面向的是广告主为其搭建数仓
## 整个数据链路
整个数据源由两个部分组成
- 广告主平台有来自于自身的一些关于广告投放相关的数据存储在mysql中
- 广告投放平台有一些关于广告曝光点击的监测数据，是日志文件
### 1. ad_scripts是数据装载过程中的一些脚本文件
- 广告相关数据是由DataX将mysql中的数据传输到hdfs当中，对应的脚本是ad_scipts/ad_mysql_to_hdfs_full.sh
- ods：直接将原始数据（本来存储在hdfs的/orgin_date/ad/的文件夹）传到ad相关数仓中另一个文件夹/warehouse/ad/
- ods到dim：将ods中的一些维度信息传入dim层，其中包含由广告信息，平台信息
- dim到dwd：维度退化，将所用到的表合并成一张事实表
- 由于hive计算依赖于mr，计算速度较慢，所以将hive在hdfs上的数据传入到列式存储的clickhouse中，方便后续的指标计算数据分析工作
### 2. ad_sql是数仓分层中，各分层的建表语句，以及装载数据的测试
- 重点是dwd层的ip以及ua的解析，写了两个udf函数并使用，udf具体的编写在/ad_hive_udf目录
- dwd层有一些异常曝光点击数据的过滤操作，比如同一ip/设备访问过r快，同一ip/设备相同周期访问多次
- mysql到hdfs用了dataX，dataX有自己的json编写格式，写了一个脚本按照mysql的表中字段自动生成json，目录在/datax_genjson
### 3. 用Dophinshedule进行调度
- 采用DS来对数据装载和处理过程进行调度，生成一个完整的DAG，主要用到的脚本文件在ad_scripts
### 4. 一些思考
- 因为基于MR的hive在计算过程中可能出现小文件处理问题，将其计算框架换成hive on spark，spark可以动态合并小文件，并且spark基于内存可以减少IO
- 最后将数据从hive导入到clickhouse中，hive适用于批处理主要用于ETL过程，但是最后的dwd层中的事实表是为了方便下游进行数据分析或者一些可视化的看板操作，需要进行实时查询，做到秒级甚至毫秒级的查询，而clickhouse是列式存储，可以更好地适应下游数据分析操作
- 其中dwd层的中事实表中事件发生时间的字段event_time为什么要用解析ua的url中的时间戳来确定，因为广告点击时间与日志文件收到监测数据上报请求时间会有时间差（数据漂移），flume——>kafka——>flume，所以不能直接用time_local时间，要从request_url字段当中解析出广告点击时间
