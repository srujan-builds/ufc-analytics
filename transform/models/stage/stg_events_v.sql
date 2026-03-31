with raw_ufc_events as (
    select * from {{ source('raw_data', 'raw_ufc_events') }}
)

select distinct
    trim(event_name) as event_name,
    event_date,
    event_location,
    extracted_at
from raw_ufc_events
