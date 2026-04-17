{{ config(
    materialized='incremental',
    unique_key=['seller_id', 'stat_date']
) }}

with source as (
    select * from {{ ref('stg_seller_daily_stats') }}
),

dim_seller as (
    select seller_key, seller_id
    from {{ ref('dim_seller') }}
),

dim_date as (
    select date_key, date
    from {{ ref('dim_date') }}
),

joined as (
    select
        ds.seller_key,
        s.seller_id,
        dd.date_key,
        s.stat_date,
        s.gmv,
        s.total_orders,
        s.cancelled_orders,
        s.returned_orders,
        s.total_items,
        s.cancellation_rate,
        s.return_rate,
        s.avg_rating
    from source s
    join dim_seller ds on s.seller_id = ds.seller_id
    join dim_date dd on s.stat_date = dd.date
)

select * from joined

{% if is_incremental() %}
    where stat_date > (select max(stat_date) from {{ this }})
{% endif %}
