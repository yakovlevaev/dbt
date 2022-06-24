{% macro get_events() %}

{% set events_query %}
select english_name 
from {{ source('yakovlevaev', 'EVENT_DICTIONARY') }}
where english_name not in ('NOT_PARSED', 'VISITS')
{% endset %}

{% set results = run_query(events_query) %}

{% if execute %}
{# Return the first column #}
{% set results_list = results.columns[0].values() %}
{% else %}
{% set results_list = [] %}
{% endif %}

{{ return(results_list) }}

{% endmacro %}