--  DIM层设计要点：
-- （1）DIM层的设计依据是维度建模理论，该层存储维度模型的维度表。
-- （2）DIM层的数据存储格式为orc列式存储+snappy压缩。
-- （3）DIM层表名的命名规范为dim_表名_全量表或者拉链表标识（full/zip）

-- 广告信息维度表
drop table if exists dim_ads_info_full;
create external table if not exists dim_ads_info_full(
    ad_id string comment "广告id",
    ad_name string comment "广告名称",
    product_id string comment "产品id",
    product_name string comment "产品名称",
    product_price decimal(16, 2) comment "产品价格",
    material_id string comment "素材id",
    material_url string comment "物料地址",
    group_id string comment "广告组id"
)partitioned by (`dt` string)
stored as orc
location '/warehouse/ad/dim/dim_ads_info_full'
tblproperties ('orc.compress'='snappy');

insert overwrite table dim_ads_info_full partition (dt='2023-01-07')
select
    ad.id,
    ad_name,
    ad.product_id,
    name,
    price,
    material_id,
    material_url,
    group_id
from (
    select
        id,
        ad_name,
        product_id,
        material_id,
        material_url,
        group_id
    from ods_ads_info_full
    where dt = '2023-01-07'
     ) ad
left join (select
               id,
               name,
               price
           from ods_product_info_full
           where dt = '2023-01-07'
) pro
on ad.product_id = pro.id;

-- 创建平台信息维度表
drop table if exists dim_platform_info_full;
create external table if not exists dim_platform_info_full(
    id string comment "平台id",
    platform_name_en string comment "平台名称（英文）",
    platform_name_zh string comment "平台名称（中文）"
)partitioned by (`dt` string)
stored as orc
location '/warehouse/ad/dim/dim_platform_info_full'
tblproperties ('orc.compress'='snappy');

insert overwrite table dim_platform_info_full partition (dt = '2023-01-07')
select id, platform_name_en,platform_name_zh
    from ods_platform_info_full
where dt = '2023-01-07';

-- 爬虫ua维度表
drop table if exists dim_crawler_user_agent;
create external table if not exists dim_crawler_user_agent(
    pattern string comment '正则表达式',
    addition_date string comment '收录日期',
    url string comment '爬虫官方url',
    instance array<string> comment 'UA实例'
)
stored as orc
location '/warehouse/ad/dim/dim_crawler_user_agent'
tblproperties ('orc.compress'='snappy');

-- 先创建一个临时表
create temporary table if not exists tmp_crawler_user_agent
(
    pattern string comment '正则表达式',
    addition_date string comment '收录日期',
    url string comment '爬虫官方url',
    instances array<string> comment 'UA实例'
)
    row format serde 'org.apache.hadoop.hive.serde2.JsonSerDe'
    stored as textfile
    location '/warehouse/ad/tmp/tmp_crawler_user_agent';

insert overwrite table dim_crawler_user_agent
select *
from tmp_crawler_user_agent;






