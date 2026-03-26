{#
  Test to validate that aggregated sums are not NULL for any date partition

  @description Validates that the SUM of an aggregate column is not NULL for any date partition. This catches partitions where all values are NULL or missing. Use for fact columns in rollup/aggregation models to ensure complete data.
  @param column_name Name of the aggregate column to test (e.g., 'total_amount', 'record_count')
  @param date_filter_column Column to use for date filtering. Defaults to 'portal_partition_daily' if not specified.
  @param date_filter_type Data type of the date filter column. Affects how date filtering is applied.
#}
{% test no_null_aggregates(model, column_name, date_filter_column="portal_partition_daily", date_filter_type="date") %}

-- The generated SQL query will return date partitions where the SUM of the specified column is NULL
-- (meaning all values in that partition are NULL or there are no values). These are the failing rows.

SELECT
    {{ date_filter_column }}
FROM
    {{ model }}
WHERE
    {{ _ext_event_date_filter(date_filter_column, data_type=date_filter_type) }} -- Apply the optional date filter
GROUP BY 1
HAVING
    SUM({{ column_name }}) IS NULL

{% endtest %}