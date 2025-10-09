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
			split(TRIM(name), ' ') [1] AS first_name,
			CASE
				WHEN cardinality(split(TRIM(name), ' ')) > 1 THEN split(TRIM(name), ' ') [cardinality(split(TRIM(name), ' '))]
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