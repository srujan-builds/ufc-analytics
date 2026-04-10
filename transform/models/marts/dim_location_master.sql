{{
    config(
        materialized='incremental',
        unique_key='location_id',
        incremental_strategy='merge'
    )
}}

with int_locations as(
    select * from {{ ref('int_locations_v') }}
)

select 
    location_id,
    city,
    state,
    country,
    extracted_at,
    current_timestamp()::timestamp_ntz(0) as updated_at
from int_locations
{%  if is_incremental() %}
    where extracted_at > (select coalesce(max(extracted_at), '1900-01-01') from {{this}})
{% endif %}

