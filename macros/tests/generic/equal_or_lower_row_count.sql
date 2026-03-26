{# 
  Validates row count behavior for joins with filtering
  
  @description Validates that row count is less than or equal to parent model. Use for joins with filtering conditions. Requires portal_partition_daily column.
  @param compare_model Parent model to compare against. Use ref() syntax, e.g., ref('parent_model_name')
  @param join_type Type of join used (e.g., 'left', 'inner'). Optional, for documentation purposes.
      
  Expected Behavior:
    - Validates that current_row_count <= parent_row_count
    - Useful for joins that include filtering conditions (e.g., LEFT JOIN with WHERE clause)
    - Catches unintended row multiplication
    
  This test fails if:
    - current_row_count > parent_row_count (row multiplication detected)
    
  This test skips if:
    - portal_partition_daily column is missing from either model
#}
{% test equal_or_lower_row_count(model, compare_model, join_type=none) %}

{% set parent_columns = adapter.get_columns_in_relation(compare_model) | map(attribute="name") | list %}
{% set current_columns = adapter.get_columns_in_relation(model) | map(attribute="name") | list %}

{# Disable test if portal_partition_daily doesn't exist in either model #}
{% if "portal_partition_daily" not in parent_columns or "portal_partition_daily" not in current_columns %}
    {{ config(enabled=false) }}
    {{ log("SKIPPED: equal_or_lower_row_count test disabled - portal_partition_daily column not found in both models", info=true) }}
{% else %}

with
    parent_model as (
        select count(*) as row_count
        from {{ compare_model }}
        where {{ _ext_event_date_filter("portal_partition_daily", data_type="date") }}
    ),
    current_model as (
        select count(*) as row_count
        from {{ model }}
        where {{ _ext_event_date_filter("portal_partition_daily", data_type="date") }}
    )
select 
    parent_model.row_count as parent_row_count, 
    current_model.row_count as current_row_count,
    current_model.row_count - parent_model.row_count as row_count_diff,
    '{{ join_type }}' as join_type,
    'FAIL: Row count increased from parent - unintended 1-to-many relationship detected' as test_result
from parent_model, current_model
where current_model.row_count > parent_model.row_count

{% endif %}

{% endtest %}
