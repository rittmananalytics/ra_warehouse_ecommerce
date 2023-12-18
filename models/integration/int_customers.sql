{% if var('ecommerce_warehouse_customer_sources') %}
{{config(materialized="table")}}


with customers_merge_list as
  (
    {% for source in var('ecommerce_warehouse_customer_sources') %}
      {% set relation_source = 'stg_' + source + '_customers' %}

      select
        '{{source}}' as source,
        *
        from {{ ref(relation_source) }}

        {% if not loop.last %}union all{% endif %}
      {% endfor %}
  )
select
  *,
  case when ({{ dbt_utils.split_part("customer_tags","','",1) }} = 'PRO'
  or {{ dbt_utils.split_part("customer_tags","','",2) }} = 'PRO'
  or {{ dbt_utils.split_part("customer_tags","','",3) }} = 'PRO'
  or {{ dbt_utils.split_part("customer_tags","','",4) }} = 'PRO')
  AND NOT
  ({{ dbt_utils.split_part("customer_tags","','",1) }} = 'REQUEST_PRO'
  or {{ dbt_utils.split_part("customer_tags","','",2) }} = 'REQUEST_PRO'
  or {{ dbt_utils.split_part("customer_tags","','",3) }} = 'REQUEST_PRO'
  or {{ dbt_utils.split_part("customer_tags","','",4) }} = 'REQUEST_PRO') is true then true else false end as is_pro
  from customers_merge_list


{% else %}


{{
    config(
        enabled=false
    )
}}


{% endif %}
