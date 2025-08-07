{% macro trino__current_timestamp() %}
    cast(current_timestamp(6) as timestamp)
{% endmacro %}