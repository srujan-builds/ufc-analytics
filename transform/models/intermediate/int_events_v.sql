with stg_events as (
    select * from {{ ref('stg_events_v') }}
)

select
    {{ generate_event_id('event_url') }} as event_id,
    event_name,
    event_url,
    case 
        when regexp_like(event_name, '^UFC [0-9]+.*') then 'Numbered PPV'
        when event_name ilike 'UFC - Ultimate%' then 'Numbered PPV'

        when event_name ilike 'UFC Fight Night%' then 'Fight Night'
        when event_name ilike 'UFC on ESPN%' then 'Fight Night'
        when event_name ilike 'UFC on ABC%' then 'Fight Night'
        when event_name ilike 'UFC on Fox%' then 'Fight Night'
        when event_name ilike 'UFC on Fx%' then 'Fight Night'
        when event_name ilike 'UFC on FUEL%' then 'Fight Night'
        when event_name ilike 'UFC Live%' then 'Fight Night'
        when event_name ilike 'UFC Live%' then 'Fight Night' 
        when event_name ilike 'UFC Macao%' then 'Fight Night' 
        when event_name ilike 'UFC: %' then 'Fight Night' 
        when event_name ilike 'Ortiz vs Shamrock%' then 'Fight Night'
        
        when event_name ilike 'The Ultimate Fighter%' then 'The Ultimate Fighter'
        when event_name ilike '%Contender Series%' then 'DWCS'
        when event_name ilike 'Road to UFC%' then 'Road to UFC'
        else 'other'
    end as event_type,
    {{ generate_location_id('event_location') }} as location_id,
    event_date,
    extracted_at
from stg_events
-- duplicate handling
qualify row_number() over (
    partition by event_url
    order by extracted_at desc
) = 1


