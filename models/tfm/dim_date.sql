with dates as (
    {{
        dbt_utils.date_spine(
            datepart="day",
            start_date="cast('1990-01-01' as date)",
            end_date="date_add(current_date(), interval 12 month)"
        )
    }}
)
select
    cast(format_date("%Y%m%d", d.date_day) as int64) as id
    ,d.date_day as date

    -- Dateparts
    ,extract(day from d.date_day) as day_of_month
    ,extract(month from d.date_day) as month_num
    ,extract(year from d.date_day) as year
    ,extract(isoweek from d.date_day) as iso_week
    

from dates as d