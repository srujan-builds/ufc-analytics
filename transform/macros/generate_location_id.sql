{% macro generate_location_id(location_col) %}

    {% set city = "trim(split(" ~ location_col ~ ", ',')[0])" %}
    {% set state = "trim(split(" ~ location_col ~ ", ',')[1])" %}
    {% set country = "trim(split(" ~ location_col ~ ", ',')[2])" %}

    case
        when {{country}} is null
            then {{ dbt_utils.generate_surrogate_key([city, country]) }}
        else 
            {{ dbt_utils.generate_surrogate_key([city, state, country]) }}
    end

{% endmacro %}