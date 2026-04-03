{{
    config(
        materialized='incremental',
        unique_key='event_id',
        incremental_strategy='merge'
    )
}}

with int_events_v as (
    select * from {{ ref('int_events_v') }}
)

select 
    event_id,
    event_name,
    event_url,
    event_type,
    event_date,
    location_id,
    extracted_at,
    current_timestamp()::timestamp_ntz(0) as updated_at
from int_events_v
{%  if is_incremental() %}
    where extracted_at > (select coalesce(max(extracted_at), '1900-01-01') from {{this}})
{% endif %}