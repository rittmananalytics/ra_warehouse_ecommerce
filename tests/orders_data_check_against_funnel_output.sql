with analytics_orders as (select date_trunc('MONTH',of.created_timestamp) as analytics_first_day_month,
of.customer_id as analytics_customer_id,
of.order_id as analytics_order_id,
ol.product_or_sample as analytics_product_or_sample,
sum(quantity) as analytics_product_quantity
from analytics.orders_fact of
join analytics.order_lines_fact ol
on   of.order_id = ol.order_id
where ol.product_or_sample in ('Product','Sample')
group by 1,2,3,4),
test_orders as (
select to_timestamp(first_day_month,'YYYY/mm/DD') as first_day_month, customer_id, order_id, sample_or_product, product_quantity
from analytics_seed.orders_data
),
test as (
select t.*,a.*
from  test_orders t
left join  analytics_orders a
on    customer_id = analytics_customer_id
and   order_id = analytics_order_id
and   sample_or_product = analytics_product_or_sample
and   first_day_month = analytics_first_day_month
),results as (
select * from test
where first_day_month != analytics_first_day_month
or customer_id != analytics_customer_id
or order_id != analytics_order_id
or sample_or_product != analytics_product_or_sample
or product_quantity != analytics_product_quantity
order by 1,2,3)
select * from results
