WITH test AS (
	SELECT
		customer_id,
		first_sample_week_ts,
		first_sample_month_ts,
		is_converted_sample,
		first_sample_product_order_week_ts,
		first_sample_product_order_month_ts,
		customer_type,
		first_sample_product_order_ts
	FROM
		analytics_seed.customer_status_test
),
calculated AS (
	SELECT
		"customer_dim"."customer_id" AS customer_id,
		(DATE("customer_dim"."first_sample_order_week_ts")) AS first_sample_order_week_date,
		(DATE("customer_dim"."first_product_or_sample_order_month_ts")) AS first_product_or_sample_order_month_date,
		(
			CASE WHEN customer_dim.customer_type = 'Converted Sample' THEN
				'Yes'
			ELSE
				'No'
			END) AS is_converted_sample,
		(DATE("customer_dim"."first_product_or_sample_order_week_ts")) AS first_product_or_sample_order_week_date,
		(DATE("customer_dim"."first_sample_order_month_ts")) AS first_sample_order_month_date,
		"customer_dim"."customer_type" AS customer_type,
		(DATE("customer_dim"."first_product_or_sample_order_ts")) AS first_product_or_sample_order_date
	FROM
		"analytics"."orders_fact" AS "orders_fact"
		INNER JOIN "analytics"."customer_dim" AS "customer_dim" ON "orders_fact"."customer_pk" = "customer_dim"."customer_pk"
)
SELECT
	test.customer_id AS test_customer_id, calculated.customer_id AS calculated_customer_id, to_date(test.first_sample_week_ts, 'DD/MM/YYYY') AS test_first_sample_week, calculated.first_sample_order_week_date AS first_sample_week, to_date(test.first_sample_month_ts, 'DD/MM/YYYY') AS test_first_sample_month, first_sample_order_month_date, test.is_converted_sample AS test_is_converted_sample, calculated.is_converted_sample = 'Yes' AS is_converted_sample, to_date(first_sample_product_order_week_ts, 'DD/MM/YYYY') AS test_first_product_or_sample_order_week_date, first_product_or_sample_order_week_date, to_date(first_sample_product_order_month_ts, 'DD/MM/YYYY') AS test_first_sample_order_month_date, first_sample_order_month_date, test.customer_type AS test_customer_type, calculated.customer_type, to_date(first_sample_product_order_ts, 'DD/MM/YYYY') AS test_first_product_or_sample_order_date, first_product_or_sample_order_date
FROM
	test
	JOIN calculated ON test.customer_id = calculated.customer_id
WHERE
	test_first_sample_week != first_sample_week
	OR test_first_sample_month != first_sample_order_month_date
	OR test_is_converted_sample != (calculated.is_converted_sample = 'Yes')
	OR test_first_product_or_sample_order_week_date != first_product_or_sample_order_week_date
	OR test_first_sample_order_month_date != first_sample_order_month_date
	OR test_customer_type != calculated.customer_type
	OR test_first_product_or_sample_order_date != first_product_or_sample_order_date
ORDER BY
	1
