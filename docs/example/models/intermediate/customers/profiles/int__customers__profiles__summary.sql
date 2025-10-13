{{
  config(
    materialized="ephemeral"
  )
}}

WITH
	int__customers__profiles__summary AS (
		SELECT
			stg__customers__profiles__clean.customer_first_name,
			stg__customers__profiles__clean.customer_id,
			stg__customers__profiles__clean.customer_last_name,
			stg__customers__profiles__clean.customer_name,
			{{ customer_segment() }} AS customer_segment,
			stg__customers__profiles__clean.customer_last_name IS NOT NULL AS has_last_name,
			sum(
				stg__customers__profiles__clean.portal_source_count
			) AS portal_source_count,
			COUNT(stg__sales__orders__standardized.order_id) AS total_orders
		FROM
			{{ ref('stg__customers__profiles__clean') }} stg__customers__profiles__clean
			LEFT JOIN {{ ref('stg__sales__orders__standardized') }} stg__sales__orders__standardized ON stg__customers__profiles__clean.customer_id = stg__sales__orders__standardized.customer_id
		GROUP BY
			stg__customers__profiles__clean.customer_first_name,
			stg__customers__profiles__clean.customer_id,
			stg__customers__profiles__clean.customer_last_name,
			stg__customers__profiles__clean.customer_name
	)
SELECT
	*
FROM
	int__customers__profiles__summary