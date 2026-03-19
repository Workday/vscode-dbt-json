{{
  config(
    materialized="ephemeral"
  )
}}

WITH
	int__sales__analytics__monthly_revenue AS (
		SELECT
			customer_first_name,
			customer_id,
			customer_name,
			date_trunc('month', datetime) AS datetime,
			order_date,
			order_id,
			ordered_at,
			sum(portal_source_count) AS portal_source_count,
			store_id,
			store_name
		FROM
			{{ ref('int__sales__orders__enriched') }}
		GROUP BY
			customer_first_name,
			customer_id,
			customer_name,
			date_trunc('month', datetime),
			order_date,
			order_id,
			ordered_at,
			store_id,
			store_name
	)
SELECT
	*
FROM
	int__sales__analytics__monthly_revenue