{% if var("ecommerce_warehouse_customer_sources") %}

{{
    config(
        unique_key='customer_pk',
        alias='customer_dim'
    )
}}


WITH customers AS
  (
  SELECT *
  FROM
     {{ ref('int_customers') }} o
),
customer_orders_aggregates as (
  SELECT *
  FROM
     {{ ref('int_customer_order_aggregates') }} o
)
select    {{ dbt_utils.surrogate_key(
          ['c.customer_id']
        ) }} as customer_pk,
        c.*,
        s.average_order_net_revenue_excl_tax,
        s.lifetime_order_net_revenue_excl_tax,
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
        FROM      customers c
LEFT JOIN customer_orders_aggregates s
ON       c.customer_id = s.customer_id


{% else %} {{config(enabled=false)}} {% endif %}
