{{
  config(
    materialized="view"
  )
}}

WITH
	mart__customers__dashboard__analytics AS (
		SELECT
			customer_id AS customer_id,
			customer_name AS customer_name,
			CASE
				WHEN total_orders >= 10 THEN 'VIP'
				WHEN total_orders >= 3 THEN 'Regular'
				ELSE 'New'
			END AS customer_segment,
			first_name AS first_name,
			last_name IS NOT NULL AS has_last_name,
			last_name AS last_name,
			portal_source_count,
			total_orders AS total_orders
		FROM
			{{ ref('int__customers__profiles__summary') }}
	)
SELECT
	*
FROM
	mart__customers__dashboard__analytics