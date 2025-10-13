{{
  config(
    materialized="ephemeral"
  )
}}

WITH
	stg__sales__stores__locations AS (
		SELECT
			1 AS portal_source_count,
			id AS store_id,
			TRIM(name) AS store_name,
			CAST(opened_at AS TIMESTAMP) AS store_opened_at,
			DATE(CAST(opened_at AS TIMESTAMP)) AS store_opened_date,
			tax_rate AS store_tax_rate,
			tax_rate * 100 AS store_tax_rate_percent
		FROM
			{{ source('development__jaffle_shop_dev_seeds','raw_stores') }}
	)
SELECT
	*
FROM
	stg__sales__stores__locations