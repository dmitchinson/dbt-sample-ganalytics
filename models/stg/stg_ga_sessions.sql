select
    *
from
    `bigquery-public-data.google_analytics_sample.ga_sessions_*`
{% if target.name == 'dev' %}
where _TABLE_SUFFIX between '20170101' and '20170731'
{% endif %}