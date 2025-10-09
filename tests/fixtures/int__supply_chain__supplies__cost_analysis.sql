{{
  config(
    materialized="ephemeral"
  )
}}

WITH
	int__supply_chain__supplies__cost_analysis AS (
		SELECT
			stg__supply_chain__supplies__inventory.cost_cents,
			stg__supply_chain__supplies__inventory.cost_dollars,
			CASE
				WHEN stg__supply_chain__supplies__inventory.cost_dollars >= 0.40 THEN 'High Cost'
				WHEN stg__supply_chain__supplies__inventory.cost_dollars >= 0.20 THEN 'Medium Cost'
				ELSE 'Low Cost'
			END AS cost_tier,
			stg__supply_chain__supplies__inventory.is_perishable,
			stg__supply_chain__supplies__inventory.portal_source_count,
			stg__products__catalog__catalog.product_name AS product_name,
			stg__products__catalog__catalog.price_dollars AS product_price_dollars,
			stg__supply_chain__supplies__inventory.product_sku,
			stg__products__catalog__catalog.product_type AS product_type,
			stg__supply_chain__supplies__inventory.supply_category,
			stg__supply_chain__supplies__inventory.supply_id,
			stg__supply_chain__supplies__inventory.supply_name,
			CASE
				WHEN stg__products__catalog__catalog.price_dollars > 0 THEN (
					CAST(
						stg__supply_chain__supplies__inventory.cost_cents AS DECIMAL(10, 2)
					) / 100.0
				) / stg__products__catalog__catalog.price_dollars
				ELSE NULL
			END AS supply_to_price_ratio
		FROM
			{{ ref('stg__supply_chain__supplies__inventory') }} stg__supply_chain__supplies__inventory
			LEFT JOIN {{ ref('stg__products__catalog__catalog') }} stg__products__catalog__catalog ON stg__supply_chain__supplies__inventory.product_sku = stg__products__catalog__catalog.product_sku
	)
SELECT
	*
FROM
	int__supply_chain__supplies__cost_analysis