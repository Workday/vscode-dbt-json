{{
  config(
    materialized="ephemeral"
  )
}}

WITH
	stg__customers__profiles__clean AS (
		SELECT
			id AS customer_id,
			TRIM(name) AS customer_name,
			SPLIT_PART(TRIM(name), ' ', 1) AS first_name,
			CASE
				WHEN CARDINALITY(SPLIT(TRIM(name), ' ')) > 1 THEN SPLIT_PART(TRIM(name), ' ', -1)
				ELSE NULL
			END AS last_name,
			1 AS portal_source_count
		FROM
			{{ source('memory__jaffle_shop_dev_seeds','raw_customers') }}
	)
SELECT
	*
FROM
	stg__customers__profiles__clean