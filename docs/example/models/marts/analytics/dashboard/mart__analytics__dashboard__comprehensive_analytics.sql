{{
  config(
    materialized="view"
  )
}}

WITH
	mart__analytics__dashboard__comprehensive_analytics AS (
		SELECT
			CASE
				WHEN mart__products__reporting__cost_efficiency.cost_efficiency_category = 'Highly Efficient' THEN 100
				WHEN mart__products__reporting__cost_efficiency.cost_efficiency_category = 'Efficient' THEN 85
				WHEN mart__products__reporting__cost_efficiency.cost_efficiency_category = 'Moderate' THEN 70
				ELSE 50
			END AS business_impact_score,
			mart__products__reporting__cost_efficiency.cost_to_price_ratio,
			mart__products__reporting__cost_efficiency.is_perishable_supply,
			mart__products__reporting__cost_efficiency.portal_source_count,
			mart__products__reporting__cost_efficiency.product_name,
			mart__products__reporting__cost_efficiency.product_price_dollars,
			mart__products__reporting__menu_analytics.product_sku,
			mart__products__reporting__menu_analytics.product_type,
			CASE
				WHEN mart__products__reporting__cost_efficiency.cost_efficiency_category = 'Highly Efficient' THEN 'Promote & Expand'
				WHEN mart__products__reporting__cost_efficiency.cost_efficiency_category = 'Efficient' THEN 'Monitor Performance'
				WHEN mart__products__reporting__cost_efficiency.cost_efficiency_category = 'Moderate' THEN 'Optimize Costs'
				ELSE 'Review Strategy'
			END AS strategic_recommendation,
			mart__products__reporting__menu_analytics.total_items_sold,
			mart__products__reporting__menu_analytics.total_orders
		FROM
			{{ ref('mart__products__reporting__menu_analytics') }} mart__products__reporting__menu_analytics
			LEFT JOIN {{ ref('mart__products__reporting__cost_efficiency') }} mart__products__reporting__cost_efficiency ON mart__products__reporting__menu_analytics.product_sku = mart__products__reporting__cost_efficiency.product_sku
	)
SELECT
	*
FROM
	mart__analytics__dashboard__comprehensive_analytics