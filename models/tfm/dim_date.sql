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
    d.date_day as date
from dates as d