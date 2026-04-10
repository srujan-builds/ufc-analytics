{{
    config(
        materialized = 'incremental',
        unique_key=['fight_id', 'fighter_id'],
        incremental_strategy = 'merge'
        
    )
}}

with int_fights as (
    select * from {{ ref('int_fights_v') }}
)

select 
    fight_id,
    event_id,
    championship_bout,
    fighter_id,
    opponent_id,
    match_outcome,
    is_winner,
    knockdowns,
    strikes_landed,
    takedowns,
    submission_attempts,
    weight_class,
    won_by,
    round_num,
    fight_time,
    extracted_at,
    current_timestamp()::timestamp_ntz(0) as updated_at
from int_fights
{% if is_incremental() %}
    where extracted_at > (select max(extracted_at) from {{this}})
{% endif %}

