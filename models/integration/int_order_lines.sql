{% if var('ecommerce_warehouse_order_lines_sources') %}
{{config(materialized="table")}}

with t_orders_merge_list as
  (
    {% for source in var('ecommerce_warehouse_order_lines_sources') %}
      {% set relation_source = 'stg_' + source + '_order_lines' %}

      select
        '{{source}}' as source,
        *
        from {{ ref(relation_source) }}

        {% if not loop.last %}union all{% endif %}
      {% endfor %}
  ),
  orders as (
    select
      *
    from
      {{ ref('int_orders') }}
  )
select l.*,
    		nvl(price,0) * nvl(quantity,0) as gross_revenue,
    		(nvl(price,0)*nvl(quantity,0))-(nvl(tax_amount,0)) as gross_revenue_excl_tax,
    		(nvl(price,0)*nvl(quantity,0))-(nvl(total_discount,0)) as net_revenue,
    		(nvl(price,0)*nvl(quantity,0))-(nvl(tax_amount,0))-(nvl(total_discount,0)) as net_revenue_excl_tax,
        {{ dbt_utils.datediff('MIN(o.created_timestamp) over (PARTITION BY l.product_id)','o.created_timestamp','month') }}
   AS months_since_first_product_order,
      {{ dbt_utils.datediff('MIN(o.created_timestamp) over (PARTITION BY l.product_id)','o.created_timestamp','week') }}
    AS weeks_since_first_product_order
from t_orders_merge_list l
join orders o
on   l.order_id = o.order_id

{% else %}

{{config(enabled=false)}}

{% endif %}
