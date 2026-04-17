{% if target.type == 'bigquery' %}

with date_spine as (
    select date
    from unnest(
        generate_date_array('2024-10-01', '2025-03-31', interval 1 day)
    ) as date
),

final as (
    select
        cast(format_date('%Y%m%d', date) as int64)  as date_key,
        date,
        extract(year from date)                      as year,
        extract(month from date)                     as month_number,
        format_date('%B', date)                      as month_name,
        extract(quarter from date)                   as quarter,
        concat('Q', cast(extract(quarter from date) as string)) as quarter_label,
        extract(dayofweek from date)                 as day_of_week,
        format_date('%A', date)                      as day_name,
        extract(dayofweek from date) in (1, 7)       as is_weekend,
        extract(week from date)                      as week_of_year
    from date_spine
)

select * from final

{% else %}

with date_spine as (
    select
        unnest(
            generate_series(
                date '2024-10-01',
                date '2025-03-31',
                interval '1 day'
            )
        )::date as date
),

final as (
    select
        cast(strftime(date, '%Y%m%d') as integer)   as date_key,
        date,
        year(date)                                   as year,
        month(date)                                  as month_number,
        strftime(date, '%B')                         as month_name,
        quarter(date)                                as quarter,
        'Q' || quarter(date)                         as quarter_label,
        weekday(date) + 1                            as day_of_week,
        strftime(date, '%A')                         as day_name,
        weekday(date) >= 5                           as is_weekend,
        week(date)                                   as week_of_year
    from date_spine
)

select * from final

{% endif %}
