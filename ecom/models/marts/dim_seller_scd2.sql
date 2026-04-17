with changes as (
    select
        seller_id,
        seller_tier,
        rating,
        change_date                                         as effective_date,
        lead(change_date) over (
            partition by seller_id
            order by change_date
        )                                                   as expiry_date
    from {{ ref('stg_seller_changes') }}
),

final as (
    select
        row_number() over (
            order by seller_id, effective_date
        )                                                   as seller_scd_key,
        seller_id,
        seller_tier,
        rating,
        effective_date,
        expiry_date,
        case
            when expiry_date is null then true
            else false
        end                                                 as is_current_flag
    from changes
)

select * from final
