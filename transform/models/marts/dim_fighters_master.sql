{{
    config(
        materialized='incremental',
        unique_key='fighter_id',
        incremental_strategy='merge'
    )
}}

with int_fighters_v as(
    select * from {{ ref('int_fighters_v') }}
)

select 
    fighter_id,
    first_name,
    last_name,
    nickname,
    fighter_url,
    height_cm,
    weight_lbs,
    reach_inch,
    stance,
    extracted_at,
    current_timestamp()::timestamp_ntz(0) as updated_at
from int_fighters_v
{%  if is_incremental() %}
    where extracted_at > (select coalesce(max(extracted_at), '1900-01-01') from {{this}})
{% endif %}
