with raw_ufc_fighters as (
    select * from {{ source('raw_data', 'raw_ufc_fighters') }}
)

select distinct
    trim(first_name) as first_name,
    trim(last_name) as last_name,
    trim(nickname) as nickname,
    trim(fighter_url) as fighter_url,
    replace(trim(height),'--', 'NA') as height,
    replace(trim(weight),'--','NA') as weight,
    replace(trim(stance),'--', 'NA') as stance,
    replace(trim(reach),'--','NA') as reach,
    extracted_at
from raw_ufc_fighters