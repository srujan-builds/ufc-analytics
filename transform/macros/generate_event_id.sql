{%  macro generate_event_id(event_col) %}
    {{ dbt_utils.generate_surrogate_key([event_col]) }}
{% endmacro %}