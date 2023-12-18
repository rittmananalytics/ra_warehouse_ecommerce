{% if var('ecommerce_warehouse_customer_sources') %}
{{config(materialized="view")}}

with orders as (

    select *
    from {{ ref('int_orders') }}

),
    order_lines as (

    select *
    from {{ ref('int_order_lines') }}


  ),
aggregated as (

    select
        orders.customer_id,
        avg(order_lines.net_revenue_excl_tax) as average_order_net_revenue_excl_tax,
        sum(order_lines.net_revenue_excl_tax) as lifetime_order_net_revenue_excl_tax
    from orders orders
    join order_lines order_lines
    on orders.order_id = order_lines.order_id
    where customer_id is not null
    group by 1

),
segments as (

  select
    customer_id,
    first_order_billing_address_city,
    first_order_is_gift_card,
    first_order_product_title,
    first_order_product_type,
    first_order_order_landing_site_base_url,
    first_order_billing_address_country_code,
    first_order_referring_site,
    last_order_billing_address_city,
    last_order_is_gift_card,
    last_order_product_title,
    last_order_product_type,
    last_order_Product_or_Sample,
    last_order_order_landing_site_base_url,
    last_order_billing_address_country_code,
    last_order_referring_site
  from (
    select
        orders.customer_id,
        first_value(orders.billing_address_city) over (partition by orders.customer_id order by created_timestamp rows between unbounded preceding and unbounded following) as first_order_billing_address_city,
        first_value(order_lines.is_gift_card) over (partition by orders.customer_id order by created_timestamp rows between unbounded preceding and unbounded following) as first_order_is_gift_card,
        first_value(order_lines.title) over (partition by orders.customer_id order by created_timestamp, quantity rows between unbounded preceding and unbounded following) as first_order_product_title,
        first_value(order_lines.product_type) over (partition by orders.customer_id order by created_timestamp, quantity  rows between unbounded preceding and unbounded following) as first_order_product_type,
        first_value(orders.landing_site_base_url) over (partition by orders.customer_id order by created_timestamp rows between unbounded preceding and unbounded following) as first_order_order_landing_site_base_url,
        first_value(orders.billing_address_country_code) over (partition by orders.customer_id order by created_timestamp rows between unbounded preceding and unbounded following) as first_order_billing_address_country_code,
        first_value(orders.referring_site) over (partition by orders.customer_id order by created_timestamp rows between unbounded preceding and unbounded following) as first_order_referring_site,
        last_value(orders.billing_address_city) over (partition by orders.customer_id order by created_timestamp rows between unbounded preceding and unbounded following) as last_order_billing_address_city,
        last_value(order_lines.is_gift_card) over (partition by orders.customer_id order by created_timestamp rows between unbounded preceding and unbounded following) as last_order_is_gift_card,
        last_value(order_lines.title) over (partition by orders.customer_id order by created_timestamp, quantity rows between unbounded preceding and unbounded following) as last_order_product_title,
        last_value(order_lines.product_type) over (partition by orders.customer_id order by created_timestamp, quantity rows between unbounded preceding and unbounded following ) as last_order_product_type,
        last_value(order_lines.Product_or_Sample) over (partition by orders.customer_id order by created_timestamp, quantity rows between unbounded preceding and unbounded following) as last_order_Product_or_Sample,
        last_value(orders.landing_site_base_url) over (partition by orders.customer_id order by created_timestamp rows between unbounded preceding and unbounded following) as last_order_order_landing_site_base_url,
        last_value(orders.billing_address_country_code) over (partition by orders.customer_id order by created_timestamp rows between unbounded preceding and unbounded following) as last_order_billing_address_country_code,
        last_value(orders.referring_site) over (partition by orders.customer_id order by created_timestamp rows between unbounded preceding and unbounded following) as last_order_referring_site
    from orders orders
    join order_lines order_lines
    on orders.order_id = order_lines.order_id
    where customer_id is not null

  )
  {{ dbt_utils.group_by(16) }}
),
joined as (

  select
    a.customer_id,
    a.average_order_net_revenue_excl_tax,
    a.lifetime_order_net_revenue_excl_tax,
    s.first_order_billing_address_city,
    s.first_order_is_gift_card,
    s.first_order_product_title,
    s.first_order_product_type,
    s.first_order_order_landing_site_base_url,
    s.first_order_billing_address_country_code,
    s.first_order_referring_site,
    s.last_order_billing_address_city,
    s.last_order_is_gift_card,
    s.last_order_product_title,
    s.last_order_product_type,
    s.last_order_Product_or_Sample,
    s.last_order_order_landing_site_base_url,
    s.last_order_billing_address_country_code,
    s.last_order_referring_site
  from aggregated a
  join segments s
   on a.customer_id = s.customer_id
)
select *
from joined

{% else %}

{{
    config(
        enabled=false
    )
}}


{% endif %}
