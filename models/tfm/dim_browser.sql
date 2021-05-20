select distinct
    {{ dbt_utils.surrogate_key(['device.browser']) }} as id
    ,device.browser
    ,{{ dbt_utils.current_timestamp() }} as ts_load
from
    {{ ref('stg_ga_sessions') }}