{{
  config(
    materialized="ephemeral"
  )
}}

WITH
	int__customers__profiles__summary AS (
		SELECT
			customer_id AS customer_id,
			customer_name AS customer_name,
			CASE
				WHEN CAST(customer_id AS INTEGER) <= 10 THEN 'VIP'
				WHEN CAST(customer_id AS INTEGER) <= 50 THEN 'Regular'
				ELSE 'New'
			END AS customer_segment,
			first_name AS first_name,
			last_name IS NOT NULL AS has_last_name,
			last_name AS last_name,
			portal_source_count
		FROM
			{{ ref('stg__customers__profiles__clean') }}
	)
SELECT
	*
FROM
	int__customers__profiles__summary