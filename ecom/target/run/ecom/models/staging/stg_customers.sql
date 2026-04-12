
  
  create view "ecom"."main_staging"."stg_customers__dbt_tmp" as (
    /*
  stg_customers
  -------------
  - Renames columns to project convention
  - Casts signup_date to DATE
  - Deduplicates re-registered customers (keeps the earliest signup per email)
  - Replaces empty city strings with NULL
  - Excludes soft-deleted records
*/

with source as (
    select * from "ecom"."raw"."customers"
),

renamed as (
    select
        customer_id,
        first_name,
        last_name,
        lower(trim(email))                      as email,
        phone,
        nullif(trim(city), '')                  as city,
        country,
        lower(customer_tier)                    as customer_tier,
        cast(signup_date as date)               as signup_date,
        cast(is_deleted as boolean)             as is_deleted
    from source
),

deduped as (
    -- Keep earliest signup per email (first registration wins)
    select *,
        row_number() over (
            partition by email
            order by signup_date asc
        ) as rn
    from renamed
),

final as (
    select
        customer_id,
        first_name,
        last_name,
        email,
        phone,
        city,
        country,
        customer_tier,
        signup_date,
        is_deleted
    from deduped
    where rn = 1
      and is_deleted = false
)

select * from final
  );
