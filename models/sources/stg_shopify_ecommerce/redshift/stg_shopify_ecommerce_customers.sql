{{config(enabled = target.type == 'redshift')}}
{% if var("ecommerce_warehouse_customer_sources") %}
{% if 'shopify_ecommerce' in var("ecommerce_warehouse_customer_sources") %}

with source as (

  select * from {{ ref('shopify__customers') }}


),
   customer_tags as (

  select * from {{ var('stg_shopify_ecommerce_fivetran_customer_tags_table') }}

),
  sample_orders as (select
    created_timestamp,
    of.customer_id as customer_id,
    of.order_id as order_id
    from {{ ref('stg_shopify_ecommerce_orders')}} of
    join {{ ref('stg_shopify_ecommerce_order_lines')}} ol
    on   of.order_id = ol.order_id
    where ol.product_or_sample = 'Sample'
    group by 1,2,3)
  ,
  sample_order_index as (
    select
      *,
      row_number() over (partition by customer_id order by created_timestamp) as sample_order_index
    from sample_orders),
  sample_first_order as (
    select
      *
    from
      sample_order_index
    where sample_order_index=1
),
  product_sample_orders as (select
    created_timestamp,
    of.customer_id as customer_id,
    of.order_id as order_id,
    coalesce(sum(case when ol.product_or_sample = 'Product' then coalesce(quantity,0) end),0) as count_product_orders,
    coalesce(sum(case when ol.product_or_sample = 'Sample' then coalesce(quantity,0) end),0) as count_sample_orders
    from {{ ref('stg_shopify_ecommerce_orders')}} of
    join {{ ref('stg_shopify_ecommerce_order_lines')}} ol
    on   of.order_id = ol.order_id
    where ol.product_or_sample in ('Product','Sample')
    group by 1,2,3),
  product_sample_order_index as (
    select
      *,
      row_number() over (partition by customer_id order by created_timestamp) as product_sample_order_index
    from product_sample_orders),
  product_sample_first_order as (
    select
      *
    from
      product_sample_order_index
    where product_sample_order_index=1
),
   product_orders as (

     select customer_id,
            coalesce(count(order_line_id),0) as count_product_orders
     from {{ ref('stg_shopify_ecommerce_orders')}} o
     join {{ ref('stg_shopify_ecommerce_order_lines')}} l
     on   o.order_id = l.order_id
     where l.product_or_sample = 'Product'
     group by 1

   ),
joined as (
    select
      c.created_timestamp ,
      c.default_address_id ,
      c.email ,
      c.customer_id ,
      c.account_state ,
      c.is_tax_exempt ,
      c.updated_timestamp ,
      c.is_verified_email ,
      c.source_relation ,
      c.first_order_timestamp ,
      c.most_recent_order_timestamp ,
      c.average_order_value ,
      c.lifetime_total_spent ,
      c.lifetime_total_refunded,
      c.lifetime_total_amount ,
      c.lifetime_count_orders ,
      s.created_timestamp as first_sample_order_ts,
      {{ dbt_utils.date_trunc('week','s.created_timestamp') }} as first_sample_order_week_ts,
      {{ dbt_utils.date_trunc('month','s.created_timestamp') }} as first_sample_order_month_ts,
      f.created_timestamp as first_product_or_sample_order_ts,
      f.count_sample_orders as product_sample_first_order_sample_orders,
      f.count_product_orders as product_sample_first_order_product_orders,
      case when f.count_sample_orders>0 then 1 else 0 end as product_sample_first_order_sample_order_ids,
      case when f.count_product_orders>0 then 1 else 0 end as product_sample_first_order_product_order_ids,
      p.count_product_orders as product_orders_product_orders,
      {{ dbt_utils.date_trunc('month','first_product_or_sample_order_ts') }} as first_product_or_sample_order_month_ts,
      {{ dbt_utils.date_trunc('week','first_product_or_sample_order_ts') }} as first_product_or_sample_order_week_ts,
      case when f.count_sample_orders >0 then 'Sample'
           when (f.count_sample_orders <1 or f.count_sample_orders is null) and f.count_product_orders >0 then 'Product'
           else null end as first_order_product_or_sample,
      case when f.count_sample_orders >0 and coalesce(p.count_product_orders,0)+coalesce(f.count_product_orders,0) =0 then 'Sample Only'
           when f.count_sample_orders >0 and coalesce(p.count_product_orders,0)+coalesce(f.count_product_orders,0) >0 then 'Converted Sample'
           when f.count_sample_orders <1 and coalesce(p.count_product_orders,0)+coalesce(f.count_product_orders,0) >0 then 'Direct'
           else null end as customer_type,
      listagg(t.value,',') as customer_tags

    from source c
    left join customer_tags t
    on c.customer_id = t.customer_id
    left join product_sample_first_order f
    on c.customer_id = f.customer_id
    left join sample_first_order s
    on c.customer_id = s.customer_id
    left join product_orders p
    on c.customer_id = p.customer_id
    {{ dbt_utils.group_by(n=27) }}
)
select * from joined

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
