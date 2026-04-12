/*
  dim_customer
  ------------
  One row per customer — this is the Type 1 (overwrite) version.
  Day 4 will replace this with a Type 2 (history-tracking) snapshot
  that records changes to city and customer_tier over time.

  Type 1 = always shows the current/latest value, no history.
  Type 2 = keeps every historical version with valid_from / valid_to dates.
*/

with source as (
    select * from {{ ref('stg_customers') }}
),

final as (
    select
        row_number() over (order by customer_id)  as customer_key,  -- surrogate key
        customer_id,                                                  -- natural key
        first_name,
        last_name,
        first_name || ' ' || last_name            as full_name,
        email,
        city,
        country,
        customer_tier,
        signup_date
    from source
)

select * from final
