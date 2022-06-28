{%- set events = get_events() -%}

with CTE AS(
select
    --date,
    --user_id,
    session_id,
    --deviceCategory,
    --utm_source,
    --utm_medium,
    --utm_campaign,
    --landingpage,
    pagepath,
    is_landing,
    {%- for event in events %}
    ifnull(sum(case when event = '{{event}}' then totalEvents end), 0) as {{event}}_TOT,
    ifnull(sum(case when event = '{{event}}' then uniqueEvents end), 0) as {{event}}_UNIQ
    {%- if not loop.last %},{% endif -%}
    {% endfor %}
    --ifnull(count(distinct case when event = '{{event}}' then session_id end) as {{event}}_SESS
from {{ source('MART_TABLES', 'EVENTS_PARSED') }}
where event not IN ('NOT_PARSED', 'VISITS')
group by 1,2,3--,4,5,6,7,8,9,10
)

select 
    *,
    FL_REG_SUCCESS_TOT + P_AGENT_REG_SUCCESS_TOT + NULL_REG_SUCCESS_TOT + REG_REQUEST_SEND_TOT AS ALL_REG_SUCCESS_TOT,
    FL_REG_SUCCESS_UNIQ + P_AGENT_REG_SUCCESS_UNIQ + NULL_REG_SUCCESS_UNIQ + REG_REQUEST_SEND_UNIQ AS ALL_REG_SUCCESS_UNIQ,
    CL_SERP_IB_REQUEST_TOT + CL_CARD_IB_REQUEST_TOT + ZBS_IB_REQUEST_TOT + NB_SERP_IB_REQUEST_TOT + NB_CARD_IB_REQUEST_TOT + IB_REQUEST_TOT AS ALL_IB_REQUEST_TOT,
    CL_SERP_IB_REQUEST_UNIQ + CL_CARD_IB_REQUEST_UNIQ + ZBS_IB_REQUEST_UNIQ + NB_SERP_IB_REQUEST_UNIQ + NB_CARD_IB_REQUEST_UNIQ + IB_REQUEST_UNIQ AS ALL_IB_REQUEST_UNIQ
from CTE