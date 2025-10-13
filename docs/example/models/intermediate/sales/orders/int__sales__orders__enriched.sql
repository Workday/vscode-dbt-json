{{
  config(
    materialized="ephemeral"
  )
}}

WITH
	int__sales__orders__enriched AS (
		SELECT
			stg__customers__profiles__clean.customer_first_name,
			stg__sales__orders__standardized.customer_id,
			stg__customers__profiles__clean.customer_name,
			stg__sales__orders__standardized.order_date,
			stg__sales__orders__standardized.order_id,
			stg__sales__orders__standardized.order_total_cents,
			CAST(
				stg__sales__orders__standardized.order_total_cents AS DECIMAL(10, 2)
			) / 100.0 AS order_total_dollars,
			stg__sales__orders__standardized.ordered_at,
			stg__sales__orders__standardized.portal_source_count,
			stg__sales__orders__standardized.store_id,
			stg__sales__stores__locations.store_name,
			stg__sales__stores__locations.store_tax_rate,
			stg__sales__orders__standardized.subtotal_cents,
			stg__sales__orders__standardized.tax_paid_cents
		FROM
			{{ ref('stg__sales__orders__standardized') }} stg__sales__orders__standardized
			LEFT JOIN {{ ref('stg__customers__profiles__clean') }} stg__customers__profiles__clean ON stg__sales__orders__standardized.customer_id = stg__customers__profiles__clean.customer_id
			LEFT JOIN {{ ref('stg__sales__stores__locations') }} stg__sales__stores__locations ON stg__sales__orders__standardized.store_id = stg__sales__stores__locations.store_id
	)
SELECT
	*
FROM
	int__sales__orders__enriched