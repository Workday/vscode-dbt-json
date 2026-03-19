{{
  config(
    materialized="ephemeral"
  )
}}

WITH
	int__sales__orders__regional_combined AS (
		SELECT
			customer_first_name,
			customer_id,
			customer_name,
			order_date,
			order_id,
			order_total_cents,
			order_total_dollars,
			ordered_at,
			portal_source_count,
			CASE
				WHEN store_name IN ('Philadelphia', 'Brooklyn') THEN 'East Coast'
				WHEN store_name IN ('San Francisco', 'Los Angeles') THEN 'West Coast'
				ELSE 'Other'
			END AS region,
			store_id,
			store_name,
			store_tax_rate,
			subtotal_cents,
			tax_paid_cents
		FROM
			{{ ref('int__sales__orders__enriched') }}
		UNION ALL
		SELECT
			customer_first_name,
			customer_id,
			customer_name,
			order_date,
			order_id,
			order_total_cents,
			order_total_dollars,
			ordered_at,
			portal_source_count,
			CASE
				WHEN store_name IN ('Philadelphia', 'Brooklyn') THEN 'East Coast'
				WHEN store_name IN ('San Francisco', 'Los Angeles') THEN 'West Coast'
				ELSE 'Other'
			END AS region,
			store_id,
			store_name,
			store_tax_rate,
			subtotal_cents,
			tax_paid_cents
		FROM
			{{ ref('int__sales__orders__enriched') }}
	)
SELECT
	*
FROM
	int__sales__orders__regional_combined