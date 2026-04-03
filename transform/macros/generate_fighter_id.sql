{%  macro generate_fighter_id(fighter_url) %}


        {{ dbt_utils.generate_surrogate_key([fighter_url]) }}

{% endmacro %}