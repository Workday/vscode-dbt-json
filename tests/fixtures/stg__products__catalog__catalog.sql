{{
  config(
    materialized="ephemeral"
  )
}}

WITH
	stg__products__catalog__catalog AS (
		SELECT
			TRIM(description) AS description,
			LOWER(TRIM(TYPE)) = 'beverage' AS is_beverage,
			LOWER(TRIM(TYPE)) = 'jaffle' AS is_jaffle,
			1 AS portal_source_count,
			price AS price_cents,
			CAST(price AS DECIMAL(10, 2)) / 100.0 AS price_dollars,
			TRIM(name) AS product_name,
			sku AS product_sku,
			TRIM(TYPE) AS product_type
		FROM
			{{ source('memory__jaffle_shop_dev_seeds','raw_products') }}
	)
SELECT
	*
FROM
	stg__products__catalog__catalog