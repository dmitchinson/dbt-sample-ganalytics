-- Does browser choice correlate with likelihood of completing a transaction?
with cte_aggregate AS (
    select
        device.browser AS Browser
        ,count(distinct(fullVisitorId)) AS DistinctVisitors
        ,coalesce(sum(totals.transactions),0) AS TotalTransactions
    from {{ ref('stg_ga_sessions') }}
    group by device.browser
)
select
    Browser
    ,DistinctVisitors  AS DistinctVisitors
    ,TotalTransactions  AS TotalTransactions
    ,round(TotalTransactions/DistinctVisitors *100, 2) as PercentageWithCompleteTransactions
from
    cte_aggregate
order by
    DistinctVisitors desc