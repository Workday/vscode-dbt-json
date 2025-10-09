{{
  config(
    materialized="ephemeral"
  )
}}

WITH
	stg__sales__stores__locations AS (
		SELECT
			CAST(opened_at AS TIMESTAMP) AS opened_at,
			DATE(CAST(opened_at AS TIMESTAMP)) AS opened_date,
			1 AS portal_source_count,
			id AS store_id,
			TRIM(name) AS store_name,
			tax_rate AS tax_rate,
			tax_rate * 100 AS tax_rate_percent
		FROM
			{{ source('memory__jaffle_shop_dev_seeds','raw_stores') }}
	)
SELECT
	*
FROM
	stg__sales__stores__locations