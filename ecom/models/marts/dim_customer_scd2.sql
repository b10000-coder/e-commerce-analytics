with changes as (
    select
        customer_id,
        city,
        customer_tier,
        effective_date,
        lead(effective_date) over (
            partition by customer_id
            order by effective_date
        )                                                   as expiry_date
    from {{ ref('stg_customer_changes') }}
),

final as (
    select
        row_number() over (
            order by customer_id, effective_date
        )                                                   as customer_scd_key,
        customer_id,
        city,
        customer_tier,
        effective_date,
        expiry_date,
        case
            when expiry_date is null then true
            else false
        end                                                 as is_current_flag
    from changes
)

select * from final
