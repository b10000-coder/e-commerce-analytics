
  
  create view "ecom"."main_staging"."stg_orders__dbt_tmp" as (
    /*
  stg_orders
  ----------
  - Key challenge: order_date arrives in two formats (ISO and dd/mm/yyyy).
    We COALESCE two TRY_STRPTIME calls to handle both safely.
  - Filters rows where the date cannot be parsed (data quality guard)
  - Filters orphaned orders (customer_id not in stg_customers)
  - Casts and standardises order_status and payment_method
*/

with source as (
    select * from "ecom"."raw"."orders"
),

date_parsed as (
    select
        order_id,
        customer_id,
        -- Try ISO first, then dd/mm/yyyy — whichever parses wins
        coalesce(
            try_strptime(order_date, '%Y-%m-%d'),
            try_strptime(order_date, '%d/%m/%Y')
        )                                       as order_date,
        order_status,
        payment_method,
        shipping_city,
        cast(discount_amount as decimal(10,2))  as discount_amount,
        nullif(trim(coupon_code), '')           as coupon_code,
        lower(platform)                         as platform
    from source
),

valid_customers as (
    select customer_id from "ecom"."main_staging"."stg_customers"
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
        o.platform
    from date_parsed o
    -- Drop unparseable dates
    where o.order_date is not null
    -- Drop orphaned customer_ids (referential integrity fix)
    and o.customer_id in (select customer_id from valid_customers)
)

select * from final
  );
