/*
  stg_orders
  ----------
  Handles two date formats and filters orphaned customer_ids.
  Uses target-conditional date parsing for DuckDB and BigQuery compatibility.
*/
with source as (
    select * from {{ source('raw', 'orders') }}
),

valid_customers as (
    select customer_id from {{ ref('stg_customers') }}
),

date_parsed as (
    select
        order_id,
        customer_id,
        {% if target.type == 'bigquery' %}
        coalesce(
            safe.parse_date('%Y-%m-%d', order_date),
            safe.parse_date('%d/%m/%Y', order_date)
        )                                       as order_date,
        {% else %}
        coalesce(
            try_strptime(order_date, '%Y-%m-%d'),
            try_strptime(order_date, '%d/%m/%Y')
        )                                       as order_date,
        {% endif %}
        order_status,
        payment_method,
        shipping_city,
        cast(discount_amount as numeric)        as discount_amount,
        nullif(trim(coupon_code), '')           as coupon_code,
        lower(platform)                         as platform,
        nullif(trim(campaign_id), '')           as campaign_id
    from source
),

final as (
    select
        o.order_id,
        o.customer_id,
        o.order_date,
        o.order_status,
        o.payment_method,
        o.shipping_city,
        o.discount_amount,
        o.coupon_code,
        o.platform,
        o.campaign_id
    from date_parsed o
    where o.order_date is not null
      and o.customer_id in (select customer_id from valid_customers)
)

select * from final
