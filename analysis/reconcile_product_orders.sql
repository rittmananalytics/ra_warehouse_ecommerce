WITH test_data AS (
	SELECT
		first_order_month,
		customer_id,
		order_id,
		sum(
			CASE WHEN sample_or_product = 'Sample' THEN
				quantity
			END) AS sample_order_quantity,
		sum(
			CASE WHEN sample_or_product = 'Product' THEN
				quantity
			END) AS product_order_quantity
	FROM
		analytics_seed.test_first_order_product_data
	GROUP BY
		1,
		2,
		3
),
looker_data AS (
	SELECT
		o.customer_id,
		o.order_id,
		sum(
			CASE WHEN product_or_sample = 'Sample' THEN
				quantity
			END) AS sample_order_quantity,
		sum(
			CASE WHEN product_or_sample = 'Product' THEN
				quantity
			END) AS product_order_quantity
	FROM
		analytics.orders_fact o
		JOIN analytics.order_lines_fact ol ON o.order_pk = ol.order_pk
	GROUP BY
		1,
		2
)
SELECT
	t.first_order_month,
	sum(t.sample_order_quantity) AS test_sample_order_quantity,
	sum(l.sample_order_quantity) AS looker_sample_order_quantity,
	sum(l.sample_order_quantity)-sum(t.sample_order_quantity) as sample_order_quantity_variance,
	round((sum(l.sample_order_quantity)-sum(t.sample_order_quantity))::float/sum(t.sample_order_quantity)*100,2) as sample_order_quantity_variance_pct,
	sum(t.product_order_quantity) AS test_product_order_quantity,
	sum(l.product_order_quantity) AS looker_product_order_quantity,
	sum(l.product_order_quantity)-sum(t.product_order_quantity) as product_order_quantity_variance,
	round((sum(l.product_order_quantity)-sum(t.product_order_quantity))::float/sum(t.product_order_quantity)*100,2) as product_order_quantity_variance_pct

FROM
	test_data t
	JOIN looker_data l ON t.customer_id = l.customer_id
		AND t.order_id = l.order_id
	GROUP BY
		1
	ORDER BY
		1
