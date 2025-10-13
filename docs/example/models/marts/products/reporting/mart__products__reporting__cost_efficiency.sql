{{
  config(
    materialized="view"
  )
}}

WITH
	mart__products__reporting__cost_efficiency AS (
		SELECT
			CASE
				WHEN cost_dollars / product_price_dollars <= 0.30 THEN 'Highly Efficient'
				WHEN cost_dollars / product_price_dollars <= 0.50 THEN 'Efficient'
				WHEN cost_dollars / product_price_dollars <= 0.70 THEN 'Moderate'
				ELSE 'Low Efficiency'
			END AS cost_efficiency_category,
			CASE
				WHEN product_price_dollars > 0 THEN cost_dollars / product_price_dollars
				ELSE NULL
			END AS cost_to_price_ratio,
			is_perishable AS is_perishable_supply,
			CASE
				WHEN is_perishable THEN 'High Risk'
				ELSE 'Low Risk'
			END AS perishable_risk_level,
			portal_source_count,
			product_name,
			product_price_dollars,
			product_sku,
			product_type,
			cost_dollars AS supply_cost_dollars
		FROM
			{{ ref('int__supply_chain__supplies__cost_analysis') }}
	)
SELECT
	*
FROM
	mart__products__reporting__cost_efficiency