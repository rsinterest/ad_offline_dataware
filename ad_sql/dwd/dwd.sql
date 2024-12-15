-- DWD层设计要点：
-- （1）DWD层的设计依据是维度建模理论，该层存储维度模型的事实表。
-- （2）DWD层的数据存储格式为orc列式存储+snappy压缩。
-- （3）DWD层表名的命名规范为dwd_数据域_表名_单分区增量全量标识（inc/full）

drop table if exists dwd_ads_event_inc;
create external table if not exists dwd_ads_event_inc
(
    event_time             bigint comment "事件时间",
    event_type             string comment "事件类型",
    ad_id                  string comment "广告id",
    ad_name                string comment "广告名称",
    ad_product_id          string comment "广告商品id",
    ad_product_name        string comment "广告商品名称",
    ad_product_price       string comment "广告商品价格",
    ad_material_id         string comment "广告素材id",
    ad_material_url        string comment "广告素材地址",
    ad_group_id            string comment '广告组id',
    platform_id            string comment '推广平台id',
    platform_name_en       string comment '推广平台名称(英文)',
    platform_name_zh       string comment '推广平台名称(中文)',
    client_country         string comment '客户端所处国家',
    client_area            string comment '客户端所处地区',
    client_province        string comment '客户端所处省份',
    client_city            string comment '客户端所处城市',
    client_ip              string comment '客户端ip地址',
    client_device_id       string comment '客户端设备id',
    client_os_type         string comment '客户端操作系统类型',
    client_os_version      string comment '客户端操作系统版本',
    client_browser_type    string comment '客户端浏览器类型',
    client_browser_version string comment '客户端浏览器版本',
    client_user_agent      string comment '客户端UA',
    is_invalid_traffic     boolean comment '是否是异常流量'
) partitioned by (`dt` string)
    stored as orc
    location '/warehouse/ad/dwd/dwd_ads_event_inc'
    tblproperties ('orc.compress' = 'snappy');

-- 初步解析日志
create temporary table coarse_parsed_log as
select parse_url(("http://www.example.com" || request_uri), 'QUERY', 't')          as event_time,
       split(parse_url(("http://www.example.com" || request_uri), 'PATH'), '/')[3] as event_type,
       parse_url(("http://www.example.com" || request_uri), "QUERY", 'id')         as ad_id,
       parse_url(("http://www.example.com" || request_uri), "QUERY", 'ip')         as client_ip,
       split(parse_url("http://www.example.com" || request_uri, 'PATH'), '/')[2]   as platform_name_en,
       reflect('java.net.URLDecoder', 'decode', parse_url('http://www.example.com' || request_uri, 'QUERY', 'ua'),
               'utf-8')                                                               client_ua,
       parse_url('http://www.example.com' || request_uri, 'QUERY', 'os_type')         client_os_type,
       parse_url('http://www.example.com' || request_uri, 'QUERY', 'device_id')       client_device_id
from ods_ad_log_inc
where dt = '2023-01-07';


select *
from coarse_parsed_log;
-- 可以调反射函数
desc function extended reflect;

create function parse_ip
    as 'com.holy.ad.hive.udf.ParseIP'
    using jar 'hdfs://hadoop102ad:8020/user/hive/warehouse/jars/ad_hive_udf-1.0-SNAPSHOT-jar-with-dependencies.jar';

select parse_url(("http://www.example.com" || request_uri), "QUERY", 'ip') as client_ip,
       parse_ip('hdfs://hadoop102ad:8020/ip2region/ip2region.xdb',
                parse_url(("http://www.example.com" || request_uri), "QUERY", 'ip'))
from ods_ad_log_inc
where dt = '2023-01-07';

create function parse_ua
    as 'com.holy.ad.hive.udf.ParseUA'
    using jar 'hdfs://hadoop102ad:8020/user/hive/warehouse/jars/ad_hive_udf-1.0-SNAPSHOT-jar-with-dependencies.jar';

-- spark引擎中有一些关于结构体的兼容性问题，改一下这个参数设置
-- 将初步解析的ip和ua转化成能够看得懂的结构体的形式
set hive.vectorized.execution.enabled=false;
create temporary table fine_parsed_log
as
select event_time,
       event_type,
       ad_id,
       platform_name_en,
       client_ip,
       client_ua,
       client_os_type,
       client_device_id,
       parse_ip('hdfs://hadoop102ad:8020/ip2region/ip2region.xdb', client_ip) region_struct,
       if(client_ua != '', parse_ua(client_ua), null) ua_struct
from coarse_parsed_log;

-- 过滤掉部分异常流量
-- 一、5分钟访问超过100次
create temporary table high_frequency_ip
as
select distinct client_ip
from (select client_ip,
             ad_id,
             event_time,
             count(1)
                   over (partition by client_ip, ad_id order by cast(event_time as bigint) range between 300000 preceding and current row ) event_count_last_5min
      from coarse_parsed_log) t1
where event_count_last_5min > 100;

-- 同一ip固定周期访问
-- 二、固定周期重复访问超过5次
-- 1. 先进行简单去重操作

-- 2. 查看这次访问和上次访问的时间差
select ad_id,
       client_ip,
       event_time,
       event_time - lag(event_time, 1, 0) over (partition by ad_id,client_ip order by event_time) as time_diff
from (select ad_id, client_ip, event_time
      from coarse_parsed_log
      group by ad_id, client_ip, event_time) t1;

-- 如果直接用ad_id,client_ip,time_diff进行聚合求数量的话
-- 可能会出现就是两次间隔时间是它与上个时间间隔是相同，但是它们两个之间的间隔是不连续的
-- 比如1673032398580， 1673032398590 这样间隔10s访问， 1673043397267，1673043397277也是间隔10s访问，但是1673032398590，1673043397267这里并不间隔10s所以并不能算相同周期连续访问次数
create temporary table cycle_ip
as
select distinct client_ip
from (select ad_id, client_ip
      from (select *, sum(tag) over (partition by ad_id, client_ip order by event_time) as sum_tag
            from (select *,
                         if(time_diff -
                            lead(time_diff, 1, 0) over (partition by ad_id, client_ip order by event_time) != 0,
                            1,
                            0) as tag
                  from (select ad_id,
                               client_ip,
                               event_time,
                               event_time -
                               lag(event_time, 1, 0)
                                   over (partition by ad_id,client_ip order by event_time) as time_diff
                        from (select ad_id, client_ip, event_time
                              from coarse_parsed_log
                              group by ad_id, client_ip, event_time) t1) t2) t3) t4
      group by ad_id, client_ip, sum_tag
      having count(*) > 5) t5;

-- 三、同一设备访问过快

create temporary table high_frequency_device
as
select distinct client_device_id
from (select client_device_id,
             ad_id,
             event_time,
             count(1)
                   over (partition by ad_id, client_device_id order by cast(event_time as bigint) range between 300000 preceding and current row) as event_count_last_5min
      from coarse_parsed_log
      where client_device_id != '') t1
where event_count_last_5min > 100;

-- 四、固定设备同一周期访问
-- 固定周期同一设备访问超过5次
create temporary table cycle_device
as
select distinct client_device_id
from (select client_device_id, ad_id, sum_tag, count(*)
      from (select *, sum(tag) over (partition by ad_id, client_device_id order by event_time) as sum_tag
            from (select *,
                         if(time_diff -
                            lead(time_diff) over (partition by ad_id, client_device_id order by event_time) != 0, 1,
                            0) as tag
                  from (select client_device_id,
                               ad_id,
                               event_time,
                               event_time -
                               lag(event_time, 1, 0)
                                   over (partition by ad_id, client_device_id order by event_time) as time_diff
                        from (select client_device_id, ad_id, event_time from coarse_parsed_log) t1) t2) t3) t4
      group by client_device_id, ad_id, sum_tag
      having count(*) > 100) t5;

select
    event_time,
    event_type,
    log.ad_id,
    ad_name,
    adinfo.product_id ad_product_id,
    adinfo.product_name ad_product_name,
    adinfo.product_price ad_product_price,
    adinfo.material_id ad_material_id,
    adinfo.material_url ad_material_url,
    adinfo.group_id ad_group_id,
    pinfo.id platform_id,
    log.platform_name_en,
    platform_name_zh,
    region_struct.country client_country,
    region_struct.area client_area,
    region_struct.province client_province,
    region_struct.city client_city,
    log.client_ip,
    log.client_device_id,
    if(log.client_os_type != '', log.client_os_type, ua_struct.os) client_os_type,
    nvl(ua_struct.osversion, '') client_os_version,
    nvl(ua_struct.browser, '') client_browser_type,
    nvl(ua_struct.browserversion, '') client_browser_version,
    client_ua client_user_agent,
    if(coalesce(hfi.client_ip, hfd.client_device_id, ci.client_ip, cd.client_device_id) is not null, true, false) is_invalid_traffic
from fine_parsed_log log
left join high_frequency_ip hfi on log.client_ip = hfi.client_ip
left join high_frequency_device hfd on log.client_device_id = hfd.client_device_id
left join cycle_ip ci on log.client_ip = ci.client_ip
left join cycle_device cd on log.client_device_id = cd.client_device_id
left join dim_crawler_user_agent cus on log.client_ua regexp cus.pattern
left join (
    select *
    from dim_ads_info_full
    where dt = '2023-01-07'
)adinfo on log.ad_id = adinfo.ad_id
left join (
    select *
    from dim_platform_info_full
    where dt = '2023-01-07'
)pinfo on log.platform_name_en = pinfo.platform_name_en;

insert overwrite table dwd_ads_event_inc partition (dt = '2023-01-07')
select
    event_time,
    event_type,
    log.ad_id,
    ad_name,
    adinfo.product_id ad_product_id,
    adinfo.product_name ad_product_name,
    adinfo.product_price ad_product_price,
    adinfo.material_id ad_material_id,
    adinfo.material_url ad_material_url,
    adinfo.group_id ad_group_id,
    pinfo.id platform_id,
    log.platform_name_en,
    platform_name_zh,
    region_struct.country client_country,
    region_struct.area client_area,
    region_struct.province client_province,
    region_struct.city client_city,
    log.client_ip,
    log.client_device_id,
    if(log.client_os_type != '', log.client_os_type, ua_struct.os) client_os_type,
    nvl(ua_struct.osversion, '') client_os_version,
    nvl(ua_struct.browser, '') client_browser_type,
    nvl(ua_struct.browserversion, '') client_browser_version,
    client_ua client_user_agent,
    if(coalesce(hfi.client_ip, hfd.client_device_id, ci.client_ip, cd.client_device_id) is not null, true, false) is_invalid_traffic
from fine_parsed_log log
left join high_frequency_ip hfi on log.client_ip = hfi.client_ip
left join high_frequency_device hfd on log.client_device_id = hfd.client_device_id
left join cycle_ip ci on log.client_ip = ci.client_ip
left join cycle_device cd on log.client_device_id = cd.client_device_id
left join dim_crawler_user_agent cus on log.client_ua regexp cus.pattern
left join (
    select *
    from dim_ads_info_full
    where dt = '2023-01-07'
)adinfo on log.ad_id = adinfo.ad_id
left join (
    select *
    from dim_platform_info_full
    where dt = '2023-01-07'
)pinfo on log.platform_name_en = pinfo.platform_name_en;







