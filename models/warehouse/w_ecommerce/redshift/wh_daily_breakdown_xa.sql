{% if var("ecommerce_warehouse_order_sources") %}

{{
    config(
        alias='daily_breakdown_xa'
    )
}}

WITH sample_orders AS (
	SELECT
		date_trunc('DAY',
			created_timestamp) AS period,
		count(DISTINCT f.order_id) AS sample_orders,
		count(DISTINCT f.customer_id) AS sample_customers
	FROM
		{{ ref('wh_orders_fact') }} f
		JOIN {{ ref('wh_order_lines_fact') }} ol ON f.order_id = ol.order_id
	WHERE
		ol.product_or_sample = 'Sample'
	GROUP BY
		1
),
product_orders AS (
	SELECT
		date_trunc('DAY',
			created_timestamp) AS period,
		count(DISTINCT f.order_id) AS product_orders,
		count(DISTINCT f.customer_id) AS product_customers
	FROM
		{{ ref('wh_orders_fact') }} f
		JOIN {{ ref('wh_order_lines_fact') }} ol ON f.order_id = ol.order_id
	WHERE
		ol.product_or_sample = 'Product'
	GROUP BY
		1
),
total_orders AS (
	SELECT
		date_trunc('DAY',
			created_timestamp) AS period,
		count(DISTINCT f.order_id) AS total_orders,
		count(DISTINCT f.customer_id) AS total_customers
	FROM
		{{ ref('wh_orders_fact') }} f
		JOIN {{ ref('wh_order_lines_fact') }} ol ON f.order_id = ol.order_id
	GROUP BY
		1
),
new_customers AS (
	SELECT
		date_trunc('DAY',
			c.first_product_or_sample_order_ts) AS period,
		count(DISTINCT f.customer_id) AS new_customers,
		count(DISTINCT CASE WHEN c.customer_type = 'Sample Only' THEN
				f.customer_id
			END) AS new_sample_only_customers,
		count(DISTINCT CASE WHEN c.customer_type = 'Converted Sample' THEN
				f.customer_id
			END) AS new_converted_sample_customers,
		count(DISTINCT CASE WHEN c.customer_type = 'Converted Sample' THEN
				f.customer_id
			END)::float / count(DISTINCT f.customer_id) AS sample_conversion,
		count(DISTINCT CASE WHEN c.customer_type in('Sample',
				'Converted Sample') THEN
				f.customer_id
			END) AS new_sample_customers,
		count(DISTINCT CASE WHEN c.customer_type = 'Direct' THEN
				f.customer_id
			END) AS new_direct_customers,
		count(DISTINCT CASE WHEN c.customer_type = 'Direct' THEN
				f.customer_id
			END)::float / count(DISTINCT f.customer_id) AS pct_direct_of_total
	FROM
		{{ ref('wh_orders_fact') }} f
		JOIN {{ ref('wh_customers_dim') }} c ON f.customer_id = c.customer_id
	WHERE
		f.customer_order_seq_number = 1
	GROUP BY
		1
)
SELECT
	o.period,
	sample_orders,
	sample_customers,
	product_orders,
	product_customers,
	total_orders,
	total_customers,
	new_customers,
	new_sample_only_customers,
	new_converted_sample_customers,
	sample_conversion,
	new_sample_customers,
	new_direct_customers,
	pct_direct_of_total
FROM
	sample_orders o
	JOIN product_orders p ON o.period = p.period
	JOIN total_orders t ON o.period = t.period
	JOIN new_customers n ON o.period = n.period

  {% else %} {{config(enabled=false)}} {% endif %}
