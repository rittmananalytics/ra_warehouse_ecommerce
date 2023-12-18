{% if var('ecommerce_warehouse_customer_sources') %}
{{config(materialized="view")}}

with order_line as (

    select *
    from {{ ref('int_order_lines') }}

), aggregated as (

    select
        order_id,
        count(*) as line_item_count
    from order_line
    group by 1

)

select *
from aggregated

{% else %}

{{
    config(
        enabled=false
    )
}}


{% endif %}
