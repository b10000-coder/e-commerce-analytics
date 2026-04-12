/*
  dim_seller
  ----------
  One row per seller. Surrogate key (seller_key) added as best practice —
  in production you never use natural keys as FK in fact tables because
  source systems can recycle or change them.
*/

with source as (
    select * from {{ ref('stg_sellers') }}
),

final as (
    select
        row_number() over (order by seller_id)  as seller_key,   -- surrogate key
        seller_id,                                                -- natural key
        seller_name,
        category,
        city,
        is_verified,
        joined_date,
        rating,
        total_reviews
    from source
)

select * from final
