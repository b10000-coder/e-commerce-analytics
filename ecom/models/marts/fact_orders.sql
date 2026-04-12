/*
  fact_orders
  -----------
  Grain: one row per order LINE ITEM (most granular fact table).

  Why line-item grain and not order grain?
  - An order can have multiple products — line-item grain lets you analyse
    revenue, margin, and flash-sale rates per product, category, and seller.
  - You can always roll up to order level with COUNT(DISTINCT order_id).

  Measures (the numbers):
    quantity, unit_price, list_price, line_revenue, discount_pct

  Foreign keys (the context — join to dims for labels):
    date_key      → dim_date
    customer_key  → dim_customer
    product_key   → dim_product
    seller_key    → dim_seller

  Degenerate dimensions (IDs with no dim table):
    order_id, order_item_id — kept directly in the fact table
*/

with orders as (
    select * from {{ ref('stg_orders') }}
),

order_items as (
    select * from {{ ref('stg_order_items') }}
),

dim_customer as (
    select customer_key, customer_id from {{ ref('dim_customer') }}
),

dim_product as (
    select product_key, product_id from {{ ref('dim_product') }}
),

dim_seller as (
    select seller_key, seller_id from {{ ref('dim_seller') }}
),

joined as (
    select
        -- degenerate dimensions (natural keys kept in fact)
        oi.order_item_id,
        o.order_id,

        -- foreign keys to dimensions
        cast(strftime(o.order_date::date, '%Y%m%d') as integer)  as date_key,
        dc.customer_key,
        dp.product_key,
        ds.seller_key,

        -- measures
        oi.quantity,
        oi.unit_price,
        oi.list_price,
        oi.discount_pct,
        oi.line_revenue,
        oi.is_flash_sale,

        -- order-level attributes (non-additive context)
        o.order_status,
        o.payment_method,
        o.shipping_city,
        o.platform,
        o.discount_amount                                         as order_discount_amount,
        o.coupon_code

    from order_items oi
    inner join orders o
        on oi.order_id = o.order_id
    left join dim_customer dc
        on o.customer_id = dc.customer_id
    left join dim_product dp
        on oi.product_id = dp.product_id
    left join dim_seller ds
        on oi.seller_id = ds.seller_id
)

select * from joined
