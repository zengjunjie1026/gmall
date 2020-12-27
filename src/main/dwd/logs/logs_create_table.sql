drop table if exists dwd_start_log;
CREATE EXTERNAL TABLE dwd_start_log(
`area_code` string COMMENT '地区编码',
`brand` string COMMENT '手机品牌',
`channel` string COMMENT '渠道',
`model` string COMMENT '手机型号',
`mid_id` string COMMENT '设备 id',
`os` string COMMENT '操作系统',
`user_id` string COMMENT '会员 id',
`version_code` string COMMENT 'app 版本号',
`entry` string COMMENT ' icon 手机图标 notice 通知 install 安装后启动',
`loading_time` bigint COMMENT '启动加载时间',
`open_ad_id` string COMMENT '广告页 ID ',
`open_ad_ms` bigint COMMENT '广告总共播放时间',
`open_ad_skip_ms` bigint COMMENT '用户跳过广告时点',
`ts` bigint COMMENT '时间'
) COMMENT '启动日志表'
PARTITIONED BY (dt string) -- 按照时间创建分区
stored as parquet -- 采用 parquet 列式存储
LOCATION '/warehouse/gmall/dwd/dwd_start_log' -- 指定在 HDFS 上存储位置
TBLPROPERTIES('parquet.compression'='lzo') -- 采用 LZO 压缩
;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table dwd_start_log partition(dt='2020-06-14') select
get_json_object(line,'$.common.ar'),
get_json_object(line,'$.common.ba'),
get_json_object(line,'$.common.ch'),
get_json_object(line,'$.common.md'),
get_json_object(line,'$.common.mid'),
get_json_object(line,'$.common.os'),
get_json_object(line,'$.common.uid'),
get_json_object(line,'$.common.vc'),
get_json_object(line,'$.start.entry'),
get_json_object(line,'$.start.loading_time'),
get_json_object(line,'$.start.open_ad_id'),
get_json_object(line,'$.start.open_ad_ms'),
get_json_object(line,'$.start.open_ad_skip_ms'),
get_json_object(line,'$.ts')
from ods_log
where dt='2020-06-14'
and get_json_object(line,'$.start') is not null;


select * from dwd_start_log where dt='2020-06-14' limit 2;

-- 页面日志表

drop table if exists dwd_page_log;
CREATE EXTERNAL TABLE dwd_page_log(
`area_code` string COMMENT '地区编码',
`brand` string COMMENT '手机品牌',
`channel` string COMMENT '渠道',
`model` string COMMENT '手机型号',
`mid_id` string COMMENT '设备 id',
`os` string COMMENT '操作系统',
`user_id` string COMMENT '会员 id',
`version_code` string COMMENT 'app 版本号',
`during_time` bigint COMMENT '持续时间毫秒',
`page_item` string COMMENT '目标 id ',
`page_item_type` string COMMENT '目标类型',
`last_page_id` string COMMENT '上页类型',
`page_id` string COMMENT '页面 ID ',
`source_type` string COMMENT '来源类型',
`ts` bigint
) COMMENT '页面日志表'
PARTITIONED BY (dt string)
stored as parquet
LOCATION '/warehouse/gmall/dwd/dwd_page_log'
TBLPROPERTIES('parquet.compression'='lzo');

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table dwd_page_log partition(dt='2020-06-14')
select
get_json_object(line,'$.common.ar'),
get_json_object(line,'$.common.ba'),
get_json_object(line,'$.common.ch'),
get_json_object(line,'$.common.md'),
get_json_object(line,'$.common.mid'), get_json_object(line,'$.common.os'),
get_json_object(line,'$.common.uid'),
get_json_object(line,'$.common.vc'),
get_json_object(line,'$.page.during_time'),
get_json_object(line,'$.page.item'),
get_json_object(line,'$.page.item_type'),
get_json_object(line,'$.page.last_page_id'),
get_json_object(line,'$.page.page_id'),
get_json_object(line,'$.page.sourceType'), get_json_object(line,'$.ts')
from ods_log
where dt='2020-06-14'
and get_json_object(line,'$.page') is not null;


select * from dwd_page_log where dt='2020-06-14' limit 2;

-- 动作日志表

drop table if exists dwd_action_log;
CREATE EXTERNAL TABLE dwd_action_log(
`area_code` string COMMENT '地区编码',
`brand` string COMMENT '手机品牌',
`channel` string COMMENT '渠道',
`model` string COMMENT '手机型号',
`mid_id` string COMMENT '设备 id',
`os` string COMMENT '操作系统',
`user_id` string COMMENT '会员 id',
`version_code` string COMMENT 'app 版本号',
`during_time` bigint COMMENT '持续时间毫秒',
`page_item` string COMMENT '目标 id ',
`page_item_type` string COMMENT '目标类型',
`last_page_id` string COMMENT '上页类型',
`page_id` string COMMENT '页面 id ',
`source_type` string COMMENT '来源类型',
`action_id` string COMMENT '动作 id',
`item` string COMMENT '目标 id ',
`item_type` string COMMENT '目标类型',
`ts` bigint COMMENT '时间'
) COMMENT '动作日志表'
PARTITIONED BY (dt string)
stored as parquet
LOCATION '/warehouse/gmall/dwd/dwd_action_log'
TBLPROPERTIES('parquet.compression'='lzo');

create function explode_json_array as 'com.atguigu.hive.udtf.ExplodeJSONArray' using jar 'hdfs://manjaro:8020/user/hive/jars/hivefunction-1.0-SNAPSHOT.jar';

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table dwd_action_log partition(dt='2020-06-14')
select
get_json_object(line,'$.common.ar'), get_json_object(line,'$.common.ba'),
get_json_object(line,'$.common.ch'),
get_json_object(line,'$.common.md'),
get_json_object(line,'$.common.mid'),
get_json_object(line,'$.common.os'),
get_json_object(line,'$.common.uid'),
get_json_object(line,'$.common.vc'),
get_json_object(line,'$.page.during_time'),
get_json_object(line,'$.page.item'), get_json_object(line,'$.page.item_type'),
get_json_object(line,'$.page.last_page_id'),
get_json_object(line,'$.page.page_id'),
get_json_object(line,'$.page.sourceType'),
get_json_object(action,'$.action_id'),
get_json_object(action,'$.item'),
get_json_object(action,'$.item_type'),
get_json_object(action,'$.ts')
from ods_log lateral view explode_json_array(get_json_object(line,'$.actions')) tmp as
action
where dt='2020-06-14'
and get_json_object(line,'$.actions') is not null;

select get_json_object(line,'$.actions') from ods_log ;


drop table if exists dwd_display_log;
CREATE EXTERNAL TABLE dwd_display_log(
`area_code` string COMMENT '地区编码',
 `brand` string COMMENT '手机品牌',
`channel` string COMMENT '渠道',
 `model` string COMMENT '手机型号',
`mid_id` string COMMENT '设备 id',
`os` string COMMENT '操作系统',
`user_id` string COMMENT '会员 id',
`version_code` string COMMENT 'app 版本号',
`during_time` bigint COMMENT 'app 版本号',
`page_item` string COMMENT '目标 id ',
`page_item_type` string COMMENT '目标类型',
 `last_page_id` string COMMENT '上页类型',
`page_id` string COMMENT '页面 ID ',
`source_type` string COMMENT '来源类型',
`ts` bigint COMMENT 'app 版本号',
`display_type` string COMMENT '曝光类型',
`item` string COMMENT '曝光对象 id ',
`item_type` string COMMENT 'app 版本号',
`order` bigint COMMENT '出现顺序') COMMENT '曝光日志表' PARTITIONED BY (dt string)
stored as parquet
LOCATION '/warehouse/gmall/dwd/dwd_display_log'
TBLPROPERTIES('parquet.compression'='lzo');


SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table dwd_display_log partition(dt='2020-06-14')
select
get_json_object(line,'$.common.ar'),
get_json_object(line,'$.common.ba'),
get_json_object(line,'$.common.ch'),
get_json_object(line,'$.common.md'),
get_json_object(line,'$.common.mid'),
get_json_object(line,'$.common.os'), get_json_object(line,'$.common.uid'),
get_json_object(line,'$.common.vc'),
get_json_object(line,'$.page.during_time'),
get_json_object(line,'$.page.item'),
get_json_object(line,'$.page.item_type'),
get_json_object(line,'$.page.last_page_id'),
get_json_object(line,'$.page.page_id'),
get_json_object(line,'$.page.sourceType'),
get_json_object(line,'$.ts'),
get_json_object(display,'$.displayType'),
get_json_object(display,'$.item'),
get_json_object(display,'$.item_type'),
get_json_object(display,'$.order')
from ods_log lateral view
explode_json_array(get_json_object(line,'$.displays')) tmp as
display
where dt='2020-06-14'
and get_json_object(line,'$.displays') is not null;


drop table if exists dwd_error_log;
CREATE EXTERNAL TABLE dwd_error_log(
`area_code` string COMMENT '地区编码',
`brand` string COMMENT '手机品牌',
`channel` string COMMENT '渠道',
`model` string COMMENT '手机型号',
`mid_id` string COMMENT '设备 id',
`os` string COMMENT '操作系统',
`user_id` string COMMENT '会员 id',
`version_code` string COMMENT 'app 版本号',
`page_item` string COMMENT '目标 id ',
`page_item_type` string COMMENT '目标类型',
`last_page_id` string COMMENT '上页类型',
`page_id` string COMMENT '页面 ID ',
`source_type` string COMMENT '来源类型',
`entry` string COMMENT ' icon 手机图标 notice 通知 install 安装后启动',
`loading_time` string COMMENT '启动加载时间',
 `open_ad_id` string COMMENT '广告页 ID ',
`open_ad_ms` string COMMENT '广告总共播放时间',
`open_ad_skip_ms` string COMMENT '用户跳过广告时点',
`actions` string COMMENT '动作',
`displays` string COMMENT '曝光',
`ts` string COMMENT '时间',
`error_code` string COMMENT '错误码',
`msg` string COMMENT '错误信息'
) COMMENT '错误日志表'
PARTITIONED BY (dt string)
stored as parquet
LOCATION '/warehouse/gmall/dwd/dwd_error_log'
TBLPROPERTIES('parquet.compression'='lzo');


SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table dwd_error_log partition(dt='2020-06-14')
select
get_json_object(line,'$.common.ar'),
get_json_object(line,'$.common.ba'),
get_json_object(line,'$.common.ch'),
get_json_object(line,'$.common.md'),
get_json_object(line,'$.common.mid'), get_json_object(line,'$.common.os'),
get_json_object(line,'$.common.uid'),
get_json_object(line,'$.common.vc'),
get_json_object(line,'$.page.item'),
get_json_object(line,'$.page.item_type'),
get_json_object(line,'$.page.last_page_id'),
get_json_object(line,'$.page.page_id'),
get_json_object(line,'$.page.sourceType'),
get_json_object(line,'$.start.entry'), get_json_object(line,'$.start.loading_time'),
get_json_object(line,'$.start.open_ad_id'),
get_json_object(line,'$.start.open_ad_ms'),
get_json_object(line,'$.start.open_ad_skip_ms'),
get_json_object(line,'$.actions'),
get_json_object(line,'$.displays'),
get_json_object(line,'$.ts'),
get_json_object(line,'$.err.error_code'),
get_json_object(line,'$.err.msg')
from ods_log
where dt='2020-06-14'
and get_json_object(line,'$.err') is not null;