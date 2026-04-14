with raw_ufc_fights as (
    select * from {{ source('raw_data', 'raw_ufc_events') }}
)

select distinct
    trim(fight_url) as fight_url,
    event_date as fight_date,
    event_location as fight_location,
    extracted_at
from raw_ufc_fights
