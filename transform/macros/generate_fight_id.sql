{%  macro generate_fight_id(fight_url) %}

    {{ dbt_utils.generate_surrogate_key([fight_url]) }}

{% endmacro %}