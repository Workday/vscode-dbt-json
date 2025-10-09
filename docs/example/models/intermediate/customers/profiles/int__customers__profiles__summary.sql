{{
  config(
    materialized="ephemeral"
  )
}}

WITH
	int__customers__profiles__summary AS (
		SELECT
			stg__customers__profiles__clean.customer_id AS customer_id,
			stg__customers__profiles__clean.customer_name AS customer_name,
			CASE
				WHEN COUNT(stg__sales__orders__standardized.order_id) >= 10 THEN 'VIP'
				WHEN COUNT(stg__sales__orders__standardized.order_id) >= 3 THEN 'Regular'
				ELSE 'New'
			END AS customer_segment,
			stg__customers__profiles__clean.first_name AS first_name,
			stg__customers__profiles__clean.last_name IS NOT NULL AS has_last_name,
			stg__customers__profiles__clean.last_name AS last_name,
			1 AS portal_source_count,
			COUNT(stg__sales__orders__standardized.order_id) AS total_orders
		FROM
			{{ ref('stg__customers__profiles__clean') }} stg__customers__profiles__clean
			LEFT JOIN {{ ref('stg__sales__orders__standardized') }} stg__sales__orders__standardized ON stg__customers__profiles__clean.customer_id = stg__sales__orders__standardized.customer_id
		GROUP BY
			stg__customers__profiles__clean.customer_id,
			stg__customers__profiles__clean.customer_name,
			stg__customers__profiles__clean.first_name,
			stg__customers__profiles__clean.last_name
	)
SELECT
	*
FROM
	int__customers__profiles__summary