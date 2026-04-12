/*
  stg_customer_changes
  --------------------
  Cleans the raw customer attribute change events.
  Each row = one point in time when a customer's city or tier changed.

  Only keeps changes for customer_ids that exist in stg_customers
  (orphaned change events for unknown customers are dropped).
*/

with source as (
    select * from {{ source('raw', 'customer_changes') }}
),

valid_customers as (
    select customer_id from {{ ref('stg_customers') }}
),

final as (
    select
        change_id,
        customer_id,
        cast(effective_date as date)    as effective_date,
        nullif(trim(city), '')          as city,
        lower(trim(customer_tier))      as customer_tier,
        lower(trim(change_reason))      as change_reason
    from source
    where customer_id in (select customer_id from valid_customers)
)

select * from final
