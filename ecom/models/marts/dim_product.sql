/*
  dim_product
  -----------
  One row per product with surrogate key.
  Includes gross_margin_pct derived in staging — ready for margin analysis.
  is_active flag preserved so analysts can filter to current catalogue.
*/

with source as (
    select * from {{ ref('stg_products') }}
),

final as (
    select
        row_number() over (order by product_id)  as product_key,  -- surrogate key
        product_id,                                                -- natural key
        product_name,
        category,
        subcategory,
        seller_id,
        list_price,
        cost_price,
        gross_margin_pct,
        is_active,
        stock_qty,
        created_at
    from source
)

select * from final
