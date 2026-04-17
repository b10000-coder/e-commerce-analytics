with source as (
    select * from {{ source('raw', 'seller_changes') }}
),

final as (
    select
        seller_id,
        lower(seller_tier)                          as seller_tier,
        cast(rating as decimal(3,2))                as rating,
        cast(change_date as date)                   as change_date
    from source
)

select * from final
