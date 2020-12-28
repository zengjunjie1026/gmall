
drop table if exists dws_uv_detail_daycount;
create external table dws_uv_detail_daycount (
`mid_id` string COMMENT '设备 id',
`brand`  string COMMENT '手机品牌',
`model`string COMMENT '手机型号',
`login_count` bigint COMMENT '活跃次数',
`page_stats` array<struct<page_id:string,page_count:bigint>> COMMENT '页面
访问统计'
) COMMENT '每日设备行为表'
partitioned by(dt string)
stored as parquet
location '/warehouse/gmall/dws/dws_uv_detail_daycount' tblproperties ("parquet.compression"="lzo");

with

tmp_start as (
select mid_id,
brand,
model,
count(*) login_count
from dwd_start_log
where dt='2020-06-14'
group by mid_id,brand,model
),


tmp_page as
(
select
mid_id,
brand,
model,
collect_set(named_struct('page_id',page_id,'page_count',page_count)) page_stats
from

(
select
mid_id,
brand,
model,
page_id,
count(*) page_count
from dwd_page_log
where dt='2020-06-14'
group by mid_id,brand,model,page_id
)tmp
group by mid_id,brand,model )


insert overwrite table dws_uv_detail_daycount  partition(dt='2020-06-14')
select
nvl(tmp_start.mid_id,tmp_page.mid_id),
nvl(tmp_start.brand,tmp_page.brand),
nvl(tmp_start.model,tmp_page.model),
tmp_start.login_count,
tmp_page.page_stats
from tmp_start
full outer join tmp_page
on tmp_start.mid_id=tmp_page.mid_id
and tmp_start.brand=tmp_page.brand
and tmp_start.model=tmp_page.model;

---------------------------------------------------------------------------------------------------------------------------------------
drop table if exists dws_user_action_daycount;
create external table dws_user_action_daycount (
user_id string comment '用户 id',
login_count bigint comment '登录次数',
cart_count bigint comment '加入购物车次数',
order_count bigint comment '下单次数',
order_amount decimal(16,2) comment '下单金额',
payment_count bigint comment '支付次数',
payment_amount decimal(16,2) comment '支付金额',
order_detail_stats array<struct<sku_id:string,sku_num:bigint,order_count:bigint,order_amount:decimal(20,2)>> comment '下单明细统计'
) COMMENT '每日会员行为'
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dws/dws_user_action_daycount/'
tblproperties ("parquet.compression"="lzo");

with

tmp_login as (
select user_id,
count(*) login_count
from dwd_start_log
where dt='2020-06-14'
and user_id is not null
group by user_id
),

tmp_cart as
(
select
user_id,
count(*) cart_count
from dwd_action_log
where dt='2020-06-14'
and user_id is not null
and action_id='cart_add'
group by user_id
),


tmp_order as
(
select
user_id,
count(*) order_count,
sum(final_total_amount) order_amount
from dwd_fact_order_info
where dt='2020-06-14' group by user_id
),

tmp_payment as
(
select
user_id,
count(*) payment_count,
sum(payment_amount) payment_amount
from dwd_fact_payment_info
where dt='2020-06-14' group by user_id
),
tmp_order_detail as
(
select
user_id,
collect_set(named_struct('sku_id',sku_id,'sku_num',sku_num,'order_count',order_count,'order_amount',order_amount)) order_stats
from
(
select
user_id,
sku_id,
sum(sku_num) sku_num,
count(*) order_count,
cast(sum(final_amount_d) as decimal(20,2)) order_amount
from dwd_fact_order_detail where dt='2020-06-14' group by user_id,sku_id
)tmp
   group by user_id
)

insert overwrite table dws_user_action_daycount partition(dt='2020-06-14')
select
tmp_login.user_id,
login_count,
nvl(cart_count,0),
nvl(order_count,0),
nvl(order_amount,0.0),
nvl(payment_count,0),
nvl(payment_amount,0.0), order_stats
from tmp_login
left join tmp_cart
on tmp_login.user_id=tmp_cart.user_id
left join tmp_order
on tmp_login.user_id=tmp_order.user_id
left join tmp_payment
on tmp_login.user_id=tmp_payment.user_id
left join tmp_order_detail
on tmp_login.user_id=tmp_order_detail.user_id;

---------------------------------------------------------------------------------------------------------------------------------------
drop table if exists dws_sku_action_daycount;
create external table dws_sku_action_daycount (
sku_id string comment 'sku_id',
order_count bigint comment '被下单次数',
order_num bigint comment '被下单件数',
 order_amount decimal(16,2) comment '被下单金额',
payment_count bigint comment '被支付次数',
payment_num bigint comment '被支付件数',
payment_amount decimal(16,2) comment '被支付金额',
 refund_count bigint comment '被退款次数',
refund_num bigint comment '被退款件数',
refund_amount decimal(16,2) comment '被退款金额',
cart_count bigint comment '被加入购物车次数',
favor_count bigint comment '被收藏次数',
appraise_good_count bigint comment '好评数',
appraise_mid_count bigint comment '中评数',
appraise_bad_count bigint comment '差评数',
appraise_default_count bigint comment '默认评价数'
) COMMENT '每日商品行为'
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dws/dws_sku_action_daycount/'
tblproperties ("parquet.compression"="lzo");

with
tmp_order as (
select sku_id,
count(*) order_count, sum(sku_num) order_num, sum(final_amount_d) order_amount
from dwd_fact_order_detail where dt='2020-06-14' group by sku_id
),
tmp_payment as
(
select
sku_id,
count(*) payment_count, sum(sku_num) payment_num, sum(final_amount_d) payment_amount
from dwd_fact_order_detail where dt='2020-06-14'
and order_id in
(
select id
from dwd_fact_order_info
where (dt='2020-06-14'
or dt=date_add('2020-06-14',-1))
and date_format(payment_time,'yyyy-MM-dd')='2020-06-14'
)
group by sku_id ),
tmp_refund as
(
select
sku_id,
count(*) refund_count, sum(refund_num) refund_num, sum(refund_amount) refund_amount
from dwd_fact_order_refund_info where dt='2020-06-14'
group by sku_id
),
tmp_cart as (
select
item sku_id,
count(*) cart_count from dwd_action_log where dt='2020-06-14' and user_id is not null and action_id='cart_add' group by item
),tmp_favor as
(
select
item sku_id,
count(*) favor_count from dwd_action_log where dt='2020-06-14' and user_id is not null and action_id='favor_add' group by item
),
tmp_appraise as (
select
sku_id,
sum(if(appraise='1201',1,0)) appraise_good_count, sum(if(appraise='1202',1,0)) appraise_mid_count, sum(if(appraise='1203',1,0)) appraise_bad_count, sum(if(appraise='1204',1,0)) appraise_default_count
from dwd_fact_comment_info where dt='2020-06-14' group by sku_id
)
insert overwrite table dws_sku_action_daycount partition(dt='2020-06-14') select
sku_id,
sum(order_count), sum(order_num), sum(order_amount), sum(payment_count), sum(payment_num), sum(payment_amount), sum(refund_count), sum(refund_num), sum(refund_amount), sum(cart_count), sum(favor_count), sum(appraise_good_count), sum(appraise_mid_count), sum(appraise_bad_count), sum(appraise_default_count)
from
(
select
sku_id, order_count, order_num, order_amount,
0 payment_count, 0 payment_num,
0 payment_amount, 0 refund_count, 0 refund_num,
0 refund_amount,
0 cart_count,
0 favor_count,
0 appraise_good_count,
0 appraise_mid_count,
0 appraise_bad_count,
0 appraise_default_count
from tmp_order
union all
select
sku_id,
0 order_count,
0 order_num,
0 order_amount, payment_count, payment_num, payment_amount,
0 refund_count,
0 refund_num,
0 refund_amount,
0 cart_count,
0 favor_count,
0 appraise_good_count,
0 appraise_mid_count,
0 appraise_bad_count,
0 appraise_default_count
from tmp_payment
union all
select
sku_id,
0 order_count,
0 order_num,
0 order_amount,
0 payment_count,
0 payment_num,
0 payment_amount, refund_count, refund_num, refund_amount,
0 cart_count,
0 favor_count,
0 appraise_good_count,
0 appraise_mid_count,
0 appraise_bad_count,
0 appraise_default_count
from tmp_refund
union all
select
sku_id,
0 order_count,
0 order_num,
0 order_amount,
0 payment_count,
0 payment_num,
0 payment_amount,
0 refund_count,
0 refund_num,
0 refund_amount, cart_count,
0 favor_count,
0 appraise_good_count,
0 appraise_mid_count,
0 appraise_bad_count,
0 appraise_default_count
from tmp_cart
   union all
   select
sku_id,
0 order_count,
0 order_num,
0 order_amount,
0 payment_count,
0 payment_num,
0 payment_amount,
0 refund_count,
0 refund_num,
0 refund_amount,
0 cart_count, favor_count,
0 appraise_good_count,
0 appraise_mid_count,
0 appraise_bad_count,
0 appraise_default_count
from tmp_favor union all select
sku_id,
0 order_count,
0 order_num,
0 order_amount,
0 payment_count,
0 payment_num,
0 payment_amount,
0 refund_count,
0 refund_num,
0 refund_amount,
0 cart_count,
0 favor_count, appraise_good_count, appraise_mid_count, appraise_bad_count, appraise_default_count
   from tmp_appraise
)tmp
group by sku_id;

-- 每日活动主题

drop table if exists dws_activity_info_daycount;
create external table dws_activity_info_daycount(
`id` string COMMENT '编号',
`activity_name` string COMMENT '活动名称',
`activity_type` string COMMENT '活动类型',
`start_time` string COMMENT '开始时间',
 `end_time` string COMMENT '结束时间',
`create_time` string COMMENT '创建时间',
`display_count` bigint COMMENT '曝光次数',
`order_count` bigint COMMENT '下单次数',
`order_amount` decimal(20,2) COMMENT '下单金额',
`payment_count` bigint COMMENT '支付次数',
`payment_amount` decimal(20,2) COMMENT '支付金额'
) COMMENT '每日活动统计'
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dws/dws_activity_info_daycount/'
tblproperties ("parquet.compression"="lzo");

with
tmp_op as
(
select activity_id,
sum(if(date_format(create_time,'yyyy-MM-dd')='2020-06-14',1,0)) order_count,
sum(if(date_format(create_time,'yyyy-MM-dd')='2020-06-14',final_total_amount, 0)) order_amount,
sum(if(date_format(payment_time,'yyyy-MM-dd')='2020-06-14',1,0)) payment_count,
sum(if(date_format(payment_time,'yyyy-MM-dd')='2020-06-14',final_total_amoun t,0)) payment_amount
from dwd_fact_order_info
where (dt='2020-06-14' or dt=date_add('2020-06-14',-1)) and activity_id is not null
group by activity_id
),
tmp_display as
(
select
item activity_id,
count(*) display_count from dwd_display_log
where dt='2020-06-14'
and item_type='activity_id' group by item
),
tmp_activity as
(
select
*
from dwd_dim_activity_info where dt='2020-06-14'
)
insert overwrite table dws_activity_info_daycount partition(dt='2020-06-14') select
nvl(tmp_op.activity_id,tmp_display.activity_id), tmp_activity.activity_name, tmp_activity.activity_type, tmp_activity.start_time,
tmp_activity.end_time, tmp_activity.create_time, tmp_display.display_count, tmp_op.order_count, tmp_op.order_amount, tmp_op.payment_count, tmp_op.payment_amount
from tmp_op
full outer join tmp_display on tmp_op.activity_id=tmp_display.activity_id
left join tmp_activity on nvl(tmp_op.activity_id,tmp_display.activity_id)=tmp_activity.id;

-- 地区统计数
drop table if exists dws_area_stats_daycount; create external table dws_area_stats_daycount(
`id` bigint COMMENT '编号', `province_name` string COMMENT '省份名称', `area_code` string COMMENT '地区编码',
`iso_code` string COMMENT 'iso 编码', `region_id` string COMMENT '地区 ID',
`region_name` string COMMENT '地区名称', `login_count` string COMMENT '活跃设备数',
`order_count` bigint COMMENT '下单次数', `order_amount` decimal(20,2) COMMENT '下单金额',
`payment_count` bigint COMMENT '支付次数',
`payment_amount` decimal(20,2) COMMENT '支付金额' ) COMMENT '每日地区统计表'
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dws/dws_area_stats_daycount/' tblproperties ("parquet.compression"="lzo");


with tmp_login as (
select area_code,
count(*) login_count from dwd_start_log where dt='2020-06-14' group by area_code
),
tmp_op as
(
select
province_id,
sum(if(date_format(create_time,'yyyy-MM-dd')='2020-06-14',1,0)) order_count,
sum(if(date_format(create_time,'yyyy-MM-dd')='2020-06-14',final_total_amount, 0)) order_amount,
sum(if(date_format(payment_time,'yyyy-MM-dd')='2020-06-14',1,0)) payment_count,
sum(if(date_format(payment_time,'yyyy-MM-dd')='2020-06-14',final_total_amount,0)) payment_amount
from dwd_fact_order_info
where (dt='2020-06-14' or dt=date_add('2020-06-14',-1)) group by province_id
)
insert overwrite table dws_area_stats_daycount partition(dt='2020-06-14') select
pro.id,
pro.province_name, pro.area_code,
pro.iso_code,
pro.region_id, pro.region_name, nvl(tmp_login.login_count,0), nvl(tmp_op.order_count,0), nvl(tmp_op.order_amount,0.0), nvl(tmp_op.payment_count,0), nvl(tmp_op.payment_amount,0.0)
from dwd_dim_base_province pro
left join tmp_login on pro.area_code=tmp_login.area_code left join tmp_op on pro.id=tmp_op.province_id;