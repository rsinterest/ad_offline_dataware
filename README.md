# ad_offline_dataware
这个项目是关于广告投放的sgg数仓项目，面向的是广告主，为其建立数仓
## 整个数据链路
整个数据源由两个部分组成
- 广告主平台有来自于自身的一些关于广告投放相关的数据存储在mysql中
- 广告投放平台有一些关于广告曝光点击的监测数据，是日志文件
### 1. ad_scipts是数据装载过程中的一些脚本文件
- 广告相关数据是由DataX将mysql中的数据传输到hdfs当中，对应的脚本是ad_scipts/ad_mysql_to_hdfs_full.sh
- ods：直接将原始数据（本来存储在hdfs的/orgin_date/ad/的文件夹）传到ad相关数仓中另一个文件夹/warehouse/ad/
- ods到dim：将ods中的一些维度信息传入dim层，其中包含由广告信息，平台信息
- dim到dwd：维度退化，将所用到的表合并成一张事实表
