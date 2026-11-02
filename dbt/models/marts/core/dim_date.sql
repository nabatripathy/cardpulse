-- Calendar dimension over the configured date spine.
with days as (
    select cast(d as date) as calendar_date
    from unnest(generate_series(
        cast('{{ var("date_spine_start") }}' as date),
        cast('{{ var("date_spine_end") }}' as date),
        interval 1 day
    )) as t(d)
)

select
    {{ date_key('calendar_date') }} as date_key,
    calendar_date,
    year(calendar_date) as year,
    quarter(calendar_date) as quarter,
    month(calendar_date) as month,
    monthname(calendar_date) as month_name,
    day(calendar_date) as day_of_month,
    isodow(calendar_date) as iso_day_of_week,
    dayname(calendar_date) as day_name,
    isodow(calendar_date) in (6, 7) as is_weekend,
    -- Fixed-date US federal holidays only (moving holidays are out of scope for the demo).
    strftime(calendar_date, '%m-%d') in ('01-01', '06-19', '07-04', '11-11', '12-25') as is_fixed_holiday,
    cast(date_trunc('month', calendar_date) as date) as month_start_date
from days
