{% if var("ecommerce_warehouse_product_sources") %}

{{
    config(
        unique_key='product_pk',
        alias='products_dim'
    )
}}


WITH products AS
  (
  SELECT *
  FROM
     {{ ref('int_products') }} o
),
  product_id_order_aggregates as
  (
    SELECT
      *
    FROM
       {{ ref('int_product_id_order_aggregates') }} o
  )
select    {{ dbt_utils.surrogate_key(
          ['p.product_id']
        ) }} as product_pk,
        p.*,
        a.first_order_timestamp as product_id_first_order_timestamp
FROM      products p
join      product_id_order_aggregates a
ON        p.product_id = a.product_id

{% else %} {{config(enabled=false)}} {% endif %}
