{{
    config(
        materialized = 'incremental',
        unique_key = 'fighter_id',
        incremental_strategy = 'delete+insert'
    )
}}

with int_fighter_stats_snapshot as (
    select * from {{ ref('int_fighter_stats_snapshot_v') }}
)

select  
    fighter_id,
    win,
    lose,
    draw,
    is_champion,
    extracted_at,
    current_timestamp()::timestamp_ntz(0) as updated_at
from int_fighter_stats_snapshot
{% if is_incremental() %}
    where extracted_at > (select max(extracted_at) from {{this}})
{% endif %}