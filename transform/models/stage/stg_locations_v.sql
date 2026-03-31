with raw_ufc_events as (
    select * from {{ source('raw_data', 'raw_ufc_events') }}
)


select
    trim(event_location) as event_location,
    split(event_location, ',') as location_part,
    extracted_at
from raw_ufc_events
