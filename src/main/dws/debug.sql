with tmp_login as
(
select
user_id,
count(*) login_count from gmall.dwd_start_log where dt='2020-06-15'
and user_id is not null group by user_id
),
tmp_cart as
(
select
user_id,
count(*) cart_count from gmall.dwd_action_log where dt='2020-06-15'
and user_id is not null and action_id='cart_add' group by user_id
),tmp_order as
(
select
user_id,
count(*) order_count, sum(final_total_amount) order_amount
from gmall.dwd_fact_order_info where dt='2020-06-15'
group by user_id
), tmp_payment as
(
select
user_id,
count(*) payment_count, sum(payment_amount) payment_amount
from gmall.dwd_fact_payment_info where dt='2020-06-15'
group by user_id
),
tmp_order_detail as
(
select
user_id,
collect_set(named_struct('sku_id',sku_id,'sku_num',sku_num,'order_count',order_count,' order_amount',order_amount)) order_stats
from
(
select
user_id,
sku_id,
sum(sku_num) sku_num,
count(*) order_count,
cast(sum(final_amount_d) as decimal(20,2)) order_amount
from gmall.dwd_fact_order_detail where dt='2020-06-15'
group by user_id,sku_id
   )tmp
   group by user_id
   )


insert overwrite table gmall.dws_user_action_daycount partition(dt='2020-06-15') select
tmp_login.user_id, login_count, nvl(cart_count,0), nvl(order_count,0), nvl(order_amount,0.0), nvl(payment_count,0), nvl(payment_amount,0.0), order_stats
from tmp_login
left outer join tmp_cart on tmp_login.user_id=tmp_cart.user_id
left outer join tmp_order on tmp_login.user_id=tmp_order.user_id
left outer join tmp_payment on tmp_login.user_id=tmp_payment.user_id
left outer join tmp_order_detail on tmp_login.user_id=tmp_order_detail.user_id;