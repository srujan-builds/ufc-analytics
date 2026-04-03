with raw_ufc_fighters as (
    select * from {{ source('raw_data', 'raw_ufc_fighters') }}
)

select distinct
    trim(first_name) as first_name,
    trim(last_name) as last_name,
    trim(nickname) as nickname,
    trim(fighter_url) as fighter_url,
    nullif(trim(height), '--') as height,
    nullif(trim(weight), '--') as weight,
    nullif(trim(stance), '--') as stance,
    nullif(trim(reach), '--') as reach,
    extracted_at
from raw_ufc_fighters