/*
  stg_order_items
  ---------------
  - Casts numeric columns
  - Adds derived column: is_flash_sale flag when unit_price < list_price
  - Filters items whose order_id no longer exists after stg_orders cleanup
*/

with source as (
    select * from {{ source('raw', 'order_items') }}
),

valid_orders as (
    select order_id from {{ ref('stg_orders') }}
),

casted as (
    -- cast first in a separate CTE so arithmetic below works on typed values
    select
        order_item_id,
        order_id,
        product_id,
        seller_id,
        cast(quantity    as integer)        as quantity,
        cast(unit_price  as decimal(10,2))  as unit_price,
        cast(list_price  as decimal(10,2))  as list_price,
        cast(discount_pct as decimal(6,4))  as discount_pct
    from source
    where order_id in (select order_id from valid_orders)
),

renamed as (
    select
        order_item_id,
        order_id,
        product_id,
        seller_id,
        quantity,
        unit_price,
        list_price,
        discount_pct,
        unit_price < list_price                                             as is_flash_sale,
        round(unit_price * quantity * (1 - coalesce(discount_pct, 0)), 2)  as line_revenue
    from casted
)

select * from renamed
