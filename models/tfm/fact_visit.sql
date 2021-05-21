select
    -- Keys
    {{ dbt_utils.surrogate_key([
        'd.visitId'
        ,'d.fullVisitorId'
        ,'d.date'
    ]) }} as id
    ,cast(d.date as int64) as fk_dim_date
    ,{{ dbt_utils.surrogate_key(['d.device.browser']) }} as fk_dim_browser
    ,{{ dbt_utils.surrogate_key([
        'd.geoNetwork.continent'
        ,'d.geoNetwork.subContinent'
        ,'d.geoNetwork.country'
        ,'d.geoNetwork.region'
        ,'d.geoNetwork.metro'
        ,'d.geoNetwork.city'
    ]) }} as fk_dim_geo
    ,{{ dbt_utils.surrogate_key(['d.device.browser']) }} as fk_dim_os

    -- Attributes
    ,TIMESTAMP_SECONDS(d.visitStartTime) as visit_start_time
    ,d.visitId as visit_id
    ,d.fullVisitorId as full_visitor_id
    ,d.totals.visits
    ,d.totals.hits
    ,d.totals.timeOnSite as time_on_site
    ,d.totals.transactions as transactions
    ,d.totals.transactionRevenue as transaction_revenue

    -- Audit
    ,{{ dbt_utils.current_timestamp() }} as ts_load
from
    {{ ref('stg_ga_sessions') }} as d