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
        order_lines.product_id,
        min(orders.created_timestamp) as first_order_timestamp
    from orders orders
    join order_lines order_lines
    on orders.order_id = order_lines.order_id
    where order_lines.title is not null
    and order_lines.Product_or_Sample = 'Product'
    group by 1
)
select *
from aggregated
