{{
  config(
    materialized="ephemeral"
  )
}}

WITH
	stg__customers__profiles__clean AS (
		SELECT
			split(TRIM(name), ' ') [1] AS customer_first_name,
			id AS customer_id,
			CASE
				WHEN cardinality(split(TRIM(name), ' ')) > 1 THEN split(TRIM(name), ' ') [cardinality(split(TRIM(name), ' '))]
				ELSE NULL
			END AS customer_last_name,
			TRIM(name) AS customer_name,
			1 AS portal_source_count
		FROM
			{{ source('development__jaffle_shop_dev_seeds','raw_customers') }}
	)
SELECT
	*
FROM
	stg__customers__profiles__clean