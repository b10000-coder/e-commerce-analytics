/*
  stg_products
  ------------
  - Casts price columns and boolean
  - Derives gross_margin_pct for later mart use
  - Filters inactive products are kept (marts can filter by is_active)
*/

with source as (
    select * from "ecom"."raw"."products"
),

final as (
    select
        product_id,
        trim(product_name)                                  as product_name,
        trim(category)                                      as category,
        trim(subcategory)                                   as subcategory,
        seller_id,
        cast(list_price as decimal(10,2))                   as list_price,
        cast(cost_price as decimal(10,2))                   as cost_price,
        round(
            (cast(list_price as decimal(10,2)) - cast(cost_price as decimal(10,2)))
            / nullif(cast(list_price as decimal(10,2)), 0),
            4
        )                                                   as gross_margin_pct,
        cast(is_active as boolean)                          as is_active,
        cast(stock_qty as integer)                          as stock_qty,
        cast(created_at as date)                            as created_at
    from source
)

select * from final