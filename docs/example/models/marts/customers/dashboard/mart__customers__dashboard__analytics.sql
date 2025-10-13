{{
  config(
    materialized="view"
  )
}}

WITH
	mart__customers__dashboard__analytics AS (
		SELECT
			customer_first_name,
			customer_id,
			customer_last_name,
			customer_name,
			CASE
				WHEN total_orders >= 10 THEN 'VIP'
				WHEN total_orders >= 3 THEN 'Regular'
				ELSE 'New'
			END AS customer_segment,
			has_last_name,
			portal_source_count,
			total_orders
		FROM
			{{ ref('int__customers__profiles__summary') }}
	)
SELECT
	*
FROM
	mart__customers__dashboard__analytics