{{
  config(
    materialized="ephemeral"
  )
}}

WITH
	int__sales__analytics__rolling_30day AS (
		SELECT
			order_date,
			count(order_id) AS order_id_count,
			sum(order_total_dollars) AS order_total_dollars_sum,
			min(order_total_dollars) AS order_total_dollars_min,
			max(order_total_dollars) AS order_total_dollars_max,
			store_id,
			store_name,
			-- partition columns
			_ext_event_date AS portal_partition_daily
		FROM
			{{ _ext_event_dates_table() }}
			INNER JOIN {{ ref('int__sales__orders__enriched') }} ON portal_partition_daily <= _ext_event_date
			AND portal_partition_daily >= date_add('day', -30, _ext_event_date)
		GROUP BY
			order_date,
			store_id,
			store_name,
			_ext_event_date
	)
SELECT
	*
FROM
	int__sales__analytics__rolling_30day