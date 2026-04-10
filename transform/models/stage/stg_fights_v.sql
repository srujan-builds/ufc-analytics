with raw_ufc_events as (
    select * from {{ source('raw_data', 'raw_ufc_events') }}
)

select 
    trim(fight_url) as fight_url,
    trim(event_url) as event_url,
    fighter_1,
    trim(fighter_1_url) as fighter_1_url,
    fighter_2,
    trim(fighter_2_url) as fighter_2_url,
    match_outcome,
    championship_bout,
    winner_name,
    trim(winner_url) as winner_url,
    kd,
    strikes,
    sub,
    td,
    weight_class,
    method,
    round_num,
    fight_time,
    extracted_at
from raw_ufc_events
