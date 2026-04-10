with raw_ufc_fighters as (
    select * from {{ source('raw_data', 'raw_ufc_fighters') }}
)

select 
    trim(fighter_url) as fighter_url,
    win,
    lose,
    draw,
    belt as is_champion,
    extracted_at
from raw_ufc_fighters


    