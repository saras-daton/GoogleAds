{% macro extract_nested_value(variable1, variable2, variable3) %}

{% if target.type =='snowflake' %}
coalesce({{variable1}}.value:{{variable2}}, 'N/A')::{{variable3}}
{% else %}
cast(coalesce({{variable1}}.{{variable2}}, 'N/A') as {{variable3}})
{% endif %}

{% endmacro %}