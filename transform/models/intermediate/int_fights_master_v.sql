with stg_fights_master as (
    select * from {{ ref('stg_fights_master_v') }}
)

select
    {{ generate_event_id('fight_url') }} as fight_id,
    fight_url,
    {{ generate_location_id('fight_location') }} as location_id,
    fight_date,
    extracted_at
from stg_fights_master
-- duplicate handling
qualify row_number() over (
    partition by fight_url
    order by extracted_at desc
) = 1


