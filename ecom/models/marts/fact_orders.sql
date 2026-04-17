/*
  fact_orders
  -----------
  Grain: one row per order LINE ITEM (most granular fact table).
  Materialization: incremental (unique_key = order_item_id)
  New in Phase 1 upgrade:
    - campaign_key FK to dim_campaign (-1 sentinel when no campaign)
    - order_date column added for incremental watermark
    - converted from table to incremental
*/
{{ config(
    materialized='incremental',
    unique_key='order_item_id'
) }}

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

dim_campaign as (
    select campaign_key, campaign_id from {{ ref('dim_campaign') }}
),

joined as (
    select
        oi.order_item_id,
        o.order_id,
        cast(strftime(o.order_date::date, '%Y%m%d') as integer)  as date_key,
        dc.customer_key,
        dp.product_key,
        ds.seller_key,
        coalesce(dcam.campaign_key, -1)                           as campaign_key,
        oi.quantity,
        oi.unit_price,
        oi.list_price,
        oi.discount_pct,
        oi.line_revenue,
        oi.is_flash_sale,
        o.order_status,
        o.payment_method,
        o.shipping_city,
        o.platform,
        o.discount_amount                                         as order_discount_amount,
        o.coupon_code,
        o.order_date
    from order_items oi
    inner join orders o
        on oi.order_id = o.order_id
    left join dim_customer dc
        on o.customer_id = dc.customer_id
    left join dim_product dp
        on oi.product_id = dp.product_id
    left join dim_seller ds
        on oi.seller_id = ds.seller_id
    left join dim_campaign dcam
        on o.campaign_id = dcam.campaign_id
)

{% if is_incremental() %}
,
watermark as (
    select coalesce(max(order_date::date), '1900-01-01'::date) as max_date
    from {{ this }}
)
{% endif %}

select joined.*
from joined
{% if is_incremental() %}
cross join watermark
where joined.order_date::date > watermark.max_date
{% endif %}
