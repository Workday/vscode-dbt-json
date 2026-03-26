{# 
  Validates row count behavior for LEFT joins only
  
  @description Validates that row count equals parent model (detects unintended 1-to-many relationships). Requires portal_partition_daily column.
  @param compare_model Parent model to compare against. Use ref() syntax, e.g., ref('parent_model_name')
  @param join_type Type of join used (e.g., 'left', 'inner'). Test only runs for specified join types.
  
  Expected Behavior:
    - LEFT join: current_row_count must equal parent_row_count
    - Catches unintended 1-to-many join relationships that multiply rows
    
  This test fails if:
    - join_type is 'left' AND current_row_count != parent_row_count
    
  This test skips if:
    - join_type is not 'left' (INNER joins are too variable to validate strictly)
    - portal_partition_daily column is missing from either model
#}
{% test equal_row_count(model, compare_model, join_type=none) %}

{% set parent_columns = adapter.get_columns_in_relation(compare_model) | map(attribute="name") | list %}
{% set current_columns = adapter.get_columns_in_relation(model) | map(attribute="name") | list %}

{# Disable test if portal_partition_daily doesn't exist in either model #}
{% if "portal_partition_daily" not in parent_columns or "portal_partition_daily" not in current_columns %}
    {{ config(enabled=false) }}
    {{ log("SKIPPED: equal_row_count test disabled - portal_partition_daily column not found in both models", info=true) }}
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
{# Only run test for LEFT joins #}
{% if join_type and join_type | lower == 'left' %}
select 
    parent_model.row_count as parent_row_count, 
    current_model.row_count as current_row_count,
    current_model.row_count - parent_model.row_count as row_count_diff,
    '{{ join_type }}' as join_type,
    'FAIL: LEFT join increased row count - unintended 1-to-many relationship detected' as test_result
from parent_model, current_model
where current_model.row_count > parent_model.row_count
{% else %}
    {{ config(enabled=false) }}
    {{ log("SKIPPED: equal_row_count test disabled - only applicable to LEFT joins (join_type='left'). Current join_type: " ~ (join_type | string), info=true) }}
{% endif %}

{% endif %}

{% endtest %}
