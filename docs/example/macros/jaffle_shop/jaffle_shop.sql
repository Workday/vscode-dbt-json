{%- macro cost_tier() -%}
    CASE WHEN stg__supply_chain__supplies__inventory.cost_dollars >= 0.40 THEN 'High Cost' WHEN stg__supply_chain__supplies__inventory.cost_dollars >= 0.20 THEN 'Medium Cost' ELSE 'Low Cost' END
{%- endmacro -%}

{%- macro customer_segment() -%}
    CASE WHEN COUNT(stg__sales__orders__standardized.order_id) >= 10 THEN 'VIP' WHEN COUNT(stg__sales__orders__standardized.order_id) >= 3 THEN 'Regular' ELSE 'New' END
{%- endmacro -%}

{%- macro supply_to_price_ratio() -%}
    CASE WHEN stg__products__catalog__catalog.product_price_dollars > 0 THEN (CAST(stg__supply_chain__supplies__inventory.cost_cents AS DECIMAL(10,2)) / 100.0) / stg__products__catalog__catalog.product_price_dollars ELSE NULL END
{%- endmacro -%}
