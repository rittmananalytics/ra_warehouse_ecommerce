{% if var('ecommerce_warehouse_order_sources') %}
{{config(materialized="table")}}

with t_orders_merge_list as
  (
    {% for source in var('ecommerce_warehouse_order_sources') %}
      {% set relation_source = 'stg_' + source + '_orders' %}

      select
        '{{source}}' as source,
        *
        from {{ ref(relation_source) }}

        {% if not loop.last %}union all{% endif %}
      {% endfor %}
  )
select
    *,
    {{ dbt_utils.datediff(
    'LAG(created_timestamp,1) over (PARTITION BY customer_id ORDER BY created_timestamp)',
    'created_timestamp',
    'day'
  ) }} AS days_since_last_order,
    {{ dbt_utils.datediff(
    'MIN(created_timestamp) over (PARTITION BY customer_id)',
    'created_timestamp',
    'month'
  ) }} AS months_since_first_order
  from
    t_orders_merge_list
{% else %}

{{config(enabled=false)}}

{% endif %}
