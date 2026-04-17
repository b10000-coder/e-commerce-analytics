with final as (
    select
        row_number() over (order by campaign_id)    as campaign_key,
        campaign_id,
        campaign_name,
        campaign_type,
        start_date,
        end_date,
        discount_pct,
        is_active,
        end_date - start_date                       as duration_days
    from {{ ref('stg_campaigns') }}
)

select * from final
