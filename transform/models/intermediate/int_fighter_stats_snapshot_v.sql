with stg_fighter_stats_snapshot as (
    select * from {{ ref('stg_fighter_stats_snapshot_v') }}
)

select 
    -- trim(fighter_url) as fighter_url,
    {{ generate_fighter_id('fighter_url') }} as fighter_id,
    win,
    lose,
    draw,
    is_champion,
    extracted_at
from stg_fighter_stats_snapshot
-- deduplicate logic
qualify row_number() over(
    partition by fighter_url
    order by extracted_at desc
) = 1


    