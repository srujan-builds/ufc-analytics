with stg_fighters_v as (
    select * from {{ ref('stg_fighters_v') }}
),

reformed_data as (
    select 
        first_name,
        last_name,
        nickname,
        fighter_url,
        height,
        regexp_substr(height, '([0-9]+)\'',1,1,'e') as height_ft_part,
        regexp_substr(height, '([0-9]+)\"',1,1,'e') as height_inch_part,
        regexp_substr(weight, '([0-9]+) lbs',1,1,'e') as weight_lbs,
        regexp_substr(reach, '([0-9]+.0)\"',1,1,'e') as reach_inch,
        stance,
        extracted_at
    from stg_fighters_v
)

select distinct
    {{ generate_fighter_id('fighter_url') }} as fighter_id,
    first_name,
    last_name,
    nickname,
    fighter_url,
    round(height_ft_part*30.48+height_inch_part*2.54,2) as height_cm,
    round(weight_lbs*1.0, 2) as weight_lbs,
    reach_inch,
    stance,
    extracted_at
from reformed_data
-- duplicate handling
qualify row_number() over (
    partition by fighter_url
    order by extracted_at desc
) = 1
