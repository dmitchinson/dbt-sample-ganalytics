select
    -- Keys
    {{ dbt_utils.surrogate_key([
        'visitId'
        ,'fullVisitorId'
        ,'date'
    ]) }} as id
    ,[date] as fk_dim_date
    ,{{ dbt_utils.surrogate_key(['device.browser']) }} as fk_dim_browser
    ,{{ dbt_utils.surrogate_key([
        'geoNetwork.continent'
        ,'geoNetwork.subContinent'
        ,'geoNetwork.country'
        ,'geoNetwork.region'
        ,'geoNetwork.metro'
        ,'geoNetwork.city'
    ]) }} as fk_dim_geo
    ,{{ dbt_utils.surrogate_key(['device.browser']) }} as fk_dim_os

    -- Attributes
    ,TIMESTAMP_SECONDS(visitStartTime) as visit_start_time
    ,visitId as visit_id
    ,fullVisitorId as full_visitor_id
    ,totals.visits
    ,totals.hits
    ,totals.timeOnSite as time_on_site
    ,totals.transactions as transactions
    ,totals.transactionRevenue as transaction_revenue

    -- Audit
    ,{{ dbt_utils.current_timestamp() }} as ts_load
from
    {{ ref('stg_ga_sessions') }}