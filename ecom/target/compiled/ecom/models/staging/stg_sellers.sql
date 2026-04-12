/*
  stg_sellers
  -----------
  - Casts joined_date to DATE
  - Casts is_verified to boolean
  - Standardises category casing
*/

with source as (
    select * from "ecom"."raw"."sellers"
),

final as (
    select
        seller_id,
        trim(seller_name)                       as seller_name,
        trim(category)                          as category,
        city,
        cast(is_verified as boolean)            as is_verified,
        cast(joined_date as date)               as joined_date,
        cast(rating as decimal(3,1))            as rating,
        cast(total_reviews as integer)          as total_reviews
    from source
)

select * from final