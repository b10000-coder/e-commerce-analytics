with source as (
    select * from {{ source('raw', 'seller_daily_stats') }}
),

final as (
    select
        seller_id,
        cast(stat_date as date)                     as stat_date,
        cast(gmv as numeric)                  as gmv,
        cast(total_orders as integer)               as total_orders,
        cast(cancelled_orders as integer)           as cancelled_orders,
        cast(returned_orders as integer)            as returned_orders,
        cast(total_items as integer)                as total_items,
        cast(cancellation_rate as numeric)     as cancellation_rate,
        cast(return_rate as numeric)           as return_rate,
        cast(avg_rating as numeric)            as avg_rating
    from source
)

select * from final
