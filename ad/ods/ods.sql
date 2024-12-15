-- ODS层的设计要点如下：
-- （1）ODS层的表结构设计依托于从业务系统同步过来的数据结构。
-- （2）ODS层要保存全部历史数据，故其压缩格式应选择压缩比较高的，此处选择gzip。
-- （3）ODS层表名的命名规范为：ods_表名_单分区增量全量标识（inc/full）。
-- 广告主内部得到的数据
-- 创建广告信息表
drop table if exists ods_ads_info_full;
create external table ods_ads_info_full
(
    id string comment "广告编号",
    product_id string comment "产品id",
    material_id string comment "素材id",
    group_id string comment "广告组id",
    ad_name string comment "广告名称",
    material_url string comment "素材地址"
)partitioned by (`dt` string)
row format delimited fields terminated by '\t'
location '/warehouse/ad/ods/ods_ads_info_full';

-- 创建推广平台表
drop table if exists ods_platform_info_full;
create external table ods_platform_info_full(
    id string comment "平台id",
    platform_name_en string comment "平台名称（英文）",
    platform_name_zh string comment "平台名称（中文）"
)partitioned by (`dt` string)
row format delimited fields terminated by '\t'
location '/warehouse/ad/ods/ods_platform_info_full';

-- 创建产品表
drop table if exists ods_product_info_full;
create external table ods_product_info_full(
    id string comment "产品id",
    name string comment "产品名称",
    price decimal(16,2) comment "产品价格"
)partitioned by (`dt` string)
row format delimited fields terminated by '\t'
location '/warehouse/ad/ods/ods_product_info_full';

-- 创建广告投放表
drop table if exists ods_ads_platform_full;
create external table ods_ads_platform_full(
    id string comment "编号",
    ad_id string comment "广告id",
    platform_id string comment "平台id",
    create_time string comment "创建时间",
    cancel_time string comment "取消时间"
)partitioned by (`dt` string)
row format delimited fields terminated by '\t'
location '/warehouse/ad/ods/ods_ads_platform_full';

-- 创建日志服务器列表
drop table if exists ods_server_host_full;
create external table if not exists ods_server_host_full(
    id string comment "编号",
    ipv4 string comment "ipv4地址"
) partitioned by (`dt` string)
row format delimited fields terminated by '\t'
location '/warehouse/ad/ods/ods_server_host_full';

-- 从广告平台那边得到的埋点日志数据
-- 创建广告监测日志表(增量更新)
drop table if exists ods_ad_log_inc;
create external table if not exists ods_ad_log_inc
(
    time_local string comment '日志服务器收到的请求的时间',
    request_method string comment 'HTTP请求方法',
    request_uri string comment '请求路径',
    status string comment '日志服务器相应状态',
    server_addr string comment '日志服务器自身ip'
) partitioned by(`dt` string)
row format delimited fields terminated by '\u0001'
location '/warehouse/ad/ods/ods_ad_log_inc';

load data inpath '/origin_data/ad/log/ad_log/2023-01-07' into table ods_ad_log_inc partition (dt = '2023-01-07');

select *
from ods_ad_log_inc
where dt = '2023-01-07';


