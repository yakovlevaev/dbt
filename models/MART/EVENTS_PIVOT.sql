{%- set events = get_events() -%}

select
    date,
    user_id,
    session_id,
    deviceCategory,
    utm_source,
    utm_medium,
    utm_campaign,
    landingpage,
    pagepath,
    is_landing,
    {%- for event in events %}
    ifnull(sum(case when event = '{{event}}' then totalEvents end), 0) as {{event}}_TOT,
    ifnull(sum(case when event = '{{event}}' then uniqueEvents end), 0) as {{event}}_UNIQ,
    ifnull(sum(case when event = '{{event}}' then 1 end), 0) as {{event}}_SESS
    {%- if not loop.last %},{% endif -%}
    {% endfor %}
from {{ source('MART_TABLES', 'EVENTS_PARSED') }}
where event not IN ('NOT_PARSED', 'VISITS')
group by 1,2,3,4,5,6,7,8,9,10