with source as (
    select * from {{ source('raw', 'campaigns') }}
),

final as (
    select
        campaign_id,
        trim(campaign_name)                         as campaign_name,
        lower(campaign_type)                        as campaign_type,
        cast(start_date as date)                    as start_date,
        cast(end_date as date)                      as end_date,
        cast(discount_pct as decimal(5,2))          as discount_pct,
        cast(is_active as boolean)                  as is_active
    from source
)

select * from final
