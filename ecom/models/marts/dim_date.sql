/*
  dim_date
  --------
  A generated date dimension covering the full data range (Oct 2024 – Mar 2025).
  No source table needed — built purely from DuckDB's generate_series().

  date_key is an integer in YYYYMMDD format (e.g. 20241015).
  This is the foreign key pattern used in fact_orders.

  Why a date dimension?
  - Lets analysts filter/group by year, month, quarter, weekday without
    writing date functions in every query.
  - A single tested source of date logic = single source of truth.
*/

with date_spine as (
    select
        unnest(
            generate_series(date '2024-10-01', date '2025-03-31', interval '1 day')
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
