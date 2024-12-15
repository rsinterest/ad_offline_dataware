#!/bin/bash
APP=ad

# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$2" ] ;then
    do_date=$2
else 
    do_date=`date -d "-1 day" +%F`
fi

dwd_ad_event_inc="
set hive.vectorized.execution.enabled=false;

--初步解析
create temporary table coarse_parsed_log as
select parse_url(('http://www.example.com' || request_uri), 'QUERY', 't')          as event_time,
       split(parse_url(('http://www.example.com' || request_uri), 'PATH'), '/')[3] as event_type,
       parse_url(('http://www.example.com' || request_uri), 'QUERY', 'id')         as ad_id,
       parse_url(('http://www.example.com' || request_uri), 'QUERY', 'ip')         as client_ip,
       split(parse_url('http://www.example.com' || request_uri, 'PATH'), '/')[2]   as platform_name_en,
       reflect('java.net.URLDecoder', 'decode', parse_url('http://www.example.com' || request_uri, 'QUERY', 'ua'),
               'utf-8')                                                               client_ua,
       parse_url('http://www.example.com' || request_uri, 'QUERY', 'os_type')         client_os_type,
       parse_url('http://www.example.com' || request_uri, 'QUERY', 'device_id')       client_device_id
from ${APP}.ods_ad_log_inc
where dt = '$do_date';

--进一步解析ip和ua
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
       ${APP}.parse_ip('hdfs://hadoop102ad:8020/ip2region/ip2region.xdb', client_ip) region_struct,
       if(client_ua != '', ${APP}.parse_ua(client_ua), null) ua_struct
from coarse_parsed_log;

--高速访问ip
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

--周期访问ip
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

--高速访问设备
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

--周期访问设备
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

--维度退化
insert overwrite table ${APP}.dwd_ads_event_inc partition (dt = '$do_date')
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
left join ${APP}.dim_crawler_user_agent cus on log.client_ua regexp cus.pattern
left join (
    select *
    from ${APP}.dim_ads_info_full
    where dt = '$do_date'
)adinfo on log.ad_id = adinfo.ad_id
left join (
    select *
    from ${APP}.dim_platform_info_full
    where dt = '$do_date'
)pinfo on log.platform_name_en = pinfo.platform_name_en;
"

case $1 in
"dwd_ad_event_inc")
    hive -e "$dwd_ad_event_inc"
;;
"all")
    hive -e "$dwd_ad_event_inc"
;;
esac


