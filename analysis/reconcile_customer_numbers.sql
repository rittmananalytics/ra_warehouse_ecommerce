WITH test_data AS (
	SELECT
		to_date(t.first_sample_product_order_month_ts,
			'DD/MM/YYYY') AS first_sample_product_order_month_ts,
		count(DISTINCT t.customer_id) AS test_customers,
		count(DISTINCT case when customer_type = 'Sample Only' then t.customer_id end) as test_sample_only_customers,
	    count(DISTINCT case when customer_type = 'Converted Sample' then t.customer_id end) as test_converted_sample_customers,
	    count(DISTINCT case when customer_type = 'Direct' then t.customer_id end) as test_direct_customers
	FROM
		analytics_seed.customer_status_test t
	GROUP BY
		1
),
looker_data AS (
	SELECT
		c.first_product_or_sample_order_month_ts AS first_product_or_sample_order_month_ts,
		count(DISTINCT c.customer_id) AS looker_customers,
		count(DISTINCT case when customer_type = 'Sample Only' then c.customer_id end) as looker_sample_only_customers,
	    count(DISTINCT case when customer_type = 'Converted Sample' then c.customer_id end) as looker_converted_sample_customers,
	    count(DISTINCT case when customer_type = 'Direct' then c.customer_id end) as looker_direct_customers
	FROM
		analytics.customer_dim c
	GROUP BY
		1
)
SELECT
	first_sample_product_order_month_ts,
	c.first_product_or_sample_order_month_ts,
	test_customers,
	looker_customers,
	looker_customers - test_customers AS customers_variance,
	round(((looker_customers - test_customers)::float/test_customers)*100,2) as customers_variance_pct,
	test_sample_only_customers,
	looker_sample_only_customers,
	looker_sample_only_customers-test_sample_only_customers as sample_only_variance_customers,
	round(((looker_sample_only_customers - test_sample_only_customers)::float/test_sample_only_customers)*100,2) as sample_only_variance_customers_pct,
	test_converted_sample_customers,
	looker_converted_sample_customers,
	looker_converted_sample_customers-test_converted_sample_customers as converted_sample_variance_customers,
	round(((looker_converted_sample_customers - test_converted_sample_customers)::float/test_converted_sample_customers)*100,2) as converted_sample_variance_customers_pct,

	test_direct_customers,
	looker_direct_customers,
	looker_direct_customers-test_direct_customers as direct_customers_variance,
	round(((looker_direct_customers - test_direct_customers)::float/test_direct_customers)*100,2) as direct_customers_variance_pct
FROM
	test_data t
	FULL OUTER JOIN looker_data c ON t.first_sample_product_order_month_ts = c.first_product_or_sample_order_month_ts
	where first_sample_product_order_month_ts != '2021-09-01'
order by 1
