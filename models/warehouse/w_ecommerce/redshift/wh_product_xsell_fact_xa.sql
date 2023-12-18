{% if var("ecommerce_warehouse_order_sources") %}
  {{ config(
    unique_key = 'product_xsell_pk',
    alias = 'product_xsell_fact_xa'
  ) }}

  WITH orders1 AS (

    SELECT
      ol.product_pk,
      p.title,
      o.customer_pk,
      DATE_TRUNC(
        'day',
        o.created_timestamp
      ) AS created_timestamp,
      o.order_pk,
      ol.net_revenue_excl_tax,
      ol.quantity
    FROM
      {{ ref('wh_orders_fact') }}
      o
      JOIN {{ ref('wh_order_lines_fact') }}
      ol
      ON o.order_pk = ol.order_pk
      JOIN {{ ref('wh_products_dim') }}
      p
      ON ol.product_pk = p.product_pk
  ),
  orders2 AS (
    SELECT
    ol.product_pk,
    p.title,
    o.customer_pk,
    DATE_TRUNC(
      'day',
      o.created_timestamp
    ) AS created_timestamp,
    o.order_pk,
    ol.net_revenue_excl_tax,
    ol.quantity
    FROM
      {{ ref('wh_orders_fact') }}
      o
      JOIN {{ ref('wh_order_lines_fact') }}
      ol
      ON o.order_pk = ol.order_pk
      JOIN {{ ref('wh_products_dim') }}
      p
      ON ol.product_pk = p.product_pk
  ),
  product_hierarchy AS (
    SELECT
      product_pk,
      title,
      product_type,
      vendor
    FROM
      analytics.products_dim
    {{ dbt_utils.group_by(4) }}
  ),
  daily_customers_orders AS (
    SELECT
      DATE_TRUNC(
        'day',
        o.created_timestamp
      ) AS created_timestamp,
      COUNT(
        DISTINCT o.customer_pk
      ) AS total_customers,
      COUNT(
        DISTINCT o.order_pk
      ) AS total_orders
    FROM
      {{ ref('wh_orders_fact') }}
      o
    {{ dbt_utils.group_by(1) }}
  ),
  crosstab AS (
    SELECT
      {{ dbt_utils.surrogate_key(
        ['orders1.created_timestamp', 'orders1.product_pk','orders2.product_pk']
      ) }} AS product_xsell_pk,
      orders1.created_timestamp AS created_timestamp,
      orders1.product_pk AS product_pk_1,
      orders2.product_pk AS product_pk_2,
      MAX(total_customers) AS all_unique_customers,
      MAX(total_orders) AS all_unique_orders,
      COUNT(
        DISTINCT orders1.customer_pk
      ) AS unique_cross_sell_customers,
      COUNT(
        DISTINCT orders1.customer_pk
      ) / MAX(total_customers) AS pct_of_total_customers,
      COUNT(
        DISTINCT orders1.order_pk
      ) AS number_of_cross_sell_orders,
      COUNT(
        DISTINCT orders1.order_pk
      ) / MAX(total_orders) AS pct_of_total_orders,
      SUM(
        orders1.net_revenue_excl_tax
      ) AS order_1_net_unit_revenue_usd,
      SUM(
        orders2.net_revenue_excl_tax
      ) AS order_2_net_unit_revenue_usd,
      SUM(
        orders1.quantity + orders2.quantity
      ) AS cross_sell_total_unit_quantity,
      SUM(
        orders1.net_revenue_excl_tax + orders2.net_revenue_excl_tax
      ) AS cross_sell_net_revenue_excl_tax
    FROM
      orders1
      LEFT JOIN orders2
      ON orders1.order_pk = orders2.order_pk
      AND orders1.title < orders2.title
      JOIN daily_customers_orders
      ON orders1.created_timestamp = daily_customers_orders.created_timestamp
    WHERE
      orders1.product_pk IS NOT NULL
      AND orders2.product_pk IS NOT NULL
    {{ dbt_utils.group_by(4) }}
  )
SELECT
  c.*,
  ph1.title as product_name_1,
  ph1.product_type as product_type_1,
  ph1.vendor as vendor_1,
  ph2.title as product_name_2,
  ph2.product_type as product_type_2,
  ph2.vendor as vendor_2
FROM
  crosstab c
JOIN
  product_hierarchy as ph1
ON
  c.product_pk_1 = ph1.product_pk
JOIN
  product_hierarchy as ph2
ON
  c.product_pk_2 = ph2.product_pk
{% else %}
  {{ config(
    enabled = false
  ) }}
{% endif %}
