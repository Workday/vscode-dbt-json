{{
  config(
    materialized="ephemeral"
  )
}}

WITH
	stg__sales__orders__standardized AS (
		SELECT
			customer AS customer_id,
			DATE(CAST(ordered_at AS TIMESTAMP)) AS order_date,
			id AS order_id,
			order_total AS order_total_cents,
			CAST(ordered_at AS TIMESTAMP) AS ordered_at,
			1 AS portal_source_count,
			store_id AS store_id,
			subtotal AS subtotal_cents,
			tax_paid AS tax_paid_cents
		FROM
			{{ source('development__jaffle_shop_dev_seeds','raw_orders') }}
	)
SELECT
	*
FROM
	stg__sales__orders__standardized