{{
    config(
        materialized='incremental',
        unique_key='fight_id',
        incremental_strategy='merge'
    )
}}

with int_events as (
    select * from {{ ref('int_fights_master_v') }}
)

select 
    fight_id,
    fight_url,
    fight_date,
    location_id,
    extracted_at,
    current_timestamp()::timestamp_ntz(0) as updated_at
from int_events
{%  if is_incremental() %}
    where extracted_at > (select coalesce(max(extracted_at), '1900-01-01') from {{this}})
{% endif %}