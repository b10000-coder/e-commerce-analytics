/*
  dim_customer_scd2  —  Slowly Changing Dimension Type 2
  -------------------------------------------------------

  WHY SCD TYPE 2?
  ---------------
  A customer's city and tier change over time. If we just overwrite the
  current value (Type 1), we lose history. That means a report asking
  "what was this customer's tier when they placed this order?" becomes
  unanswerable — we only know what they are *now*.

  Type 2 solves this by keeping every historical version of a customer
  as a separate row, each stamped with valid_from and valid_to dates.

  GRAIN: one row per customer per time period (version).

  KEY COLUMNS:
    customer_scd_key  surrogate key — unique per VERSION (not per customer)
    customer_id       natural key   — same across all versions of one customer
    valid_from        date this version became active
    valid_to          date this version was superseded (NULL = still active)
    is_current        TRUE for the version active right now
    version_number    1 = original record, 2 = first change, etc.

  HOW IT WORKS (step by step):
    1. Take the original signup record for every customer.
    2. Union with all change events from stg_customer_changes.
    3. Use LEAD() window function to look ahead to the next event date
       for the same customer — that becomes the current row's valid_to.
    4. The last version per customer has no next event, so valid_to = NULL
       and is_current = TRUE.

  FACT TABLE USAGE:
    To get the customer dimension that was active when an order was placed:
      JOIN dim_customer_scd2 scd
        ON  fact.customer_id = scd.customer_id
        AND fact.order_date  >= scd.valid_from
        AND (fact.order_date  < scd.valid_to OR scd.valid_to IS NULL)
*/

with customers as (
    select
        customer_id,
        city,
        customer_tier,
        signup_date             as effective_date,
        first_name,
        last_name,
        email
    from {{ ref('stg_customers') }}
),

changes as (
    select
        customer_id,
        city,
        customer_tier,
        effective_date,
        null                    as first_name,
        null                    as last_name,
        null                    as email
    from {{ ref('stg_customer_changes') }}
),

-- combine original records and change events into one timeline
all_versions as (
    select * from customers
    union all
    select * from changes
),

-- carry forward name/email from the original record
-- (change events don't re-supply these)
with_context as (
    select
        customer_id,
        effective_date,
        -- coalesce fills nulls in change rows with the original values
        coalesce(
            city,
            first_value(city) over (
                partition by customer_id
                order by effective_date
                rows between unbounded preceding and current row
            )
        )                       as city,
        coalesce(
            customer_tier,
            first_value(customer_tier) over (
                partition by customer_id
                order by effective_date
                rows between unbounded preceding and current row
            )
        )                       as customer_tier,
        first_value(first_name) over (
            partition by customer_id order by effective_date
        )                       as first_name,
        first_value(last_name)  over (
            partition by customer_id order by effective_date
        )                       as last_name,
        first_value(email)      over (
            partition by customer_id order by effective_date
        )                       as email,
        row_number() over (
            partition by customer_id
            order by effective_date
        )                       as version_number
    from all_versions
),

-- apply LEAD() to compute valid_to from the next version's start date
with_validity as (
    select
        customer_id,
        first_name,
        last_name,
        email,
        city,
        customer_tier,
        effective_date          as valid_from,
        lead(effective_date) over (
            partition by customer_id
            order by effective_date
        )                       as valid_to,        -- NULL on the current version
        version_number
    from with_context
),

final as (
    select
        -- surrogate key: unique per customer VERSION
        row_number() over (
            order by customer_id, valid_from
        )                       as customer_scd_key,
        customer_id,
        first_name,
        last_name,
        first_name || ' ' || last_name  as full_name,
        email,
        city,
        customer_tier,
        valid_from,
        valid_to,
        valid_to is null        as is_current,
        version_number
    from with_validity
)

select * from final
