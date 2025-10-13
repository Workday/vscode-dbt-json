{{
  config(
    materialized="ephemeral"
  )
}}

WITH
	stg__supply_chain__supplies__inventory AS (
		SELECT
			cost AS cost_cents,
			CAST(cost AS DECIMAL(10, 2)) / 100.0 AS cost_dollars,
			CASE
				WHEN perishable = TRUE THEN TRUE
				ELSE FALSE
			END AS is_perishable,
			1 AS portal_source_count,
			sku AS product_sku,
			CASE
				WHEN LOWER(name) LIKE '%cutlery%'
				OR LOWER(name) LIKE '%fork%'
				OR LOWER(name) LIKE '%knife%' THEN 'utensils'
				WHEN LOWER(name) LIKE '%napkin%'
				OR LOWER(name) LIKE '%boat%' THEN 'packaging'
				WHEN perishable = TRUE THEN 'ingredients'
				ELSE 'other'
			END AS supply_category,
			id AS supply_id,
			TRIM(name) AS supply_name
		FROM
			{{ source('development__jaffle_shop_dev_seeds','raw_supplies') }}
	)
SELECT
	*
FROM
	stg__supply_chain__supplies__inventory