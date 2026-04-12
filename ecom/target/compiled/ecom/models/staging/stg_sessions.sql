/*
  stg_sessions
  ------------
  - Null customer_id = anonymous session (kept, not filtered)
  - Derives session_duration_mins for readability
  - Casts is_converted to boolean
*/

with source as (
    select * from "ecom"."raw"."sessions"
),

final as (
    select
        session_id,
        nullif(trim(customer_id), '')               as customer_id,   -- preserve anonymous
        cast(session_date as date)                  as session_date,
        lower(platform)                             as platform,
        lower(device_type)                          as device_type,
        cast(page_views as integer)                 as page_views,
        cast(duration_seconds as integer)           as duration_seconds,
        round(cast(duration_seconds as decimal) / 60, 2) as duration_mins,
        cast(is_converted as boolean)               as is_converted,
        nullif(trim(utm_source), '')                as utm_source
    from source
)

select * from final