{{
  config(
    materialized="view"
  )
}}

WITH
	mart__products__reporting__menu_analytics AS (
		SELECT
			item_id,
			portal_source_count,
			product_sku,
			product_type,
			total_items_sold,
			total_orders
		FROM
			{{ ref('int__products__analytics__product_popularity') }}
	)
SELECT
	*
FROM
	mart__products__reporting__menu_analytics