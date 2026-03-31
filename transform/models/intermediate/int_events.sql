with stg_events_v as (
    select * from {{ ref('stg_events_v') }}
)

select 
    {{ generate_event_id('event_name') }} as event_id,
    event_name,
    case 
        when regexp_like(event_name, '^UFC [0-9]+.*') then 'Numbered PPV'
        when event_name ilike 'UFC Fight Night%' then 'Fight Night'
        when event_name ilike 'UFC on ESPN%' then 'Fight Night'
        when event_name ilike 'UFC on ABC%' then 'Fight Night'
        when event_name ilike 'UFC on Fox%' then 'Fight Night'
        when event_name ilike 'UFC on Fx%' then 'Fight Night'
        when event_name ilike 'UFC on FUEL%' then 'Fight Night'
        when event_name ilike 'The Ultimate Fighter%' then 'The Ultimate Fighter'
        else 'other'
    end as event_type,
    {{ generate_location_id('event_location') }} as location_id,
    extracted_at
from stg_events_v


