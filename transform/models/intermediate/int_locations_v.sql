with stg_locations as (
    select * from {{ ref('stg_locations_v') }}
),


location_details as (
    select distinct
    event_location,
    trim(location_part[0]) as city,
    case when location_part[2] is null then trim(location_part[0]) else trim(location_part[1]) end as state,
    case when location_part[2] is null then trim(location_part[1]) else trim(location_part[2]) end as country,
    extracted_at
from stg_locations
)


select distinct
    {{ generate_location_id('event_location') }} as location_id,
    city,
    state,
    country,
    extracted_at
from location_details
-- duplicate handling
qualify row_number() over (
    partition by city, state, country
    order by extracted_at desc
) = 1