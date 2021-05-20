{{
    config(
        materialized='incremental',
        unique_key='id'
    )
}}

select distinct 
    {{ dbt_utils.surrogate_key([
        'geoNetwork.continent'
        ,'geoNetwork.subContinent'
        ,'geoNetwork.country'
        ,'geoNetwork.region'
        ,'geoNetwork.metro'
        ,'geoNetwork.city'
    ]) }} as id
    ,geoNetwork.continent
    ,geoNetwork.subContinent as sub_continent
    ,geoNetwork.country
    ,geoNetwork.region
    ,geoNetwork.metro
    ,geoNetwork.city
    ,{{ dbt_utils.current_timestamp() }} as ts_load
from
    {{ ref('stg_ga_sessions') }}

{% if is_incremental() %}
    where TIMESTAMP_SECONDS(visitStartTime) >= (select max(ts_load) from {{ this }})
{% endif %}