WITH
SESSIONS AS (
SELECT
    date,
    session_timestamp,
    user_id,
    session_id,
    traffic_type,
    utm_source,
    utm_medium,
    utm_campaign,
    utm_content,
    utm_keyword,
    keyword_id
FROM {{ ref('SESSIONS') }}
),

PAGE_VIEWS AS (
SELECT DISTINCT dimension4
FROM {{ source('UA_REPORTS', 'PAGE_VIEWS') }}
WHERE pagepath LIKE '%novostroyki%'
),

CALL_GAINS AS (
SELECT
    session_id,
    MAX(has_sold) AS has_sold,
    SUM(calls)    AS calls,
    SUM(good_calls) AS good_calls,
    SUM(matched_calls) AS matched_calls,
    SUM(sold_calls) AS sold_calls,
    SUM(cite_gains) AS cite_gains
FROM {{ source('CALLTOUCH_ATRIBUTION', 'CALLS_FULL_ATRIBS') }}
GROUP BY 1
),

TRAFFIC_TABLE AS (
SELECT
    date,
    CASE WHEN utm_campaign like '%new-building%' THEN 'Новостройки' ELSE traffic_type END  AS traffic_type,
    utm_source,
    utm_medium,
    IFNULL(utm_campaign, '') AS utm_campaign,
    IFNULL(utm_content, '') AS utm_content,
    IFNULL(LOWER(utm_keyword), '') AS utm_keyword,
    IFNULL(keyword_id, '') AS keyword_id,
    user_id,
    COUNT(DISTINCT session_id) AS VISITS,
    COUNT(DISTINCT PAGE_VIEWS.dimension4) AS VISITS_NB,
    SUM(IFNULL(NB_NB_FREE_CLOPS_TOT, 0)) AS NB_NB_FREE_CLOPS_TOT,
    SUM(IFNULL(NB_NB_FREE_CLOPS_UNIQ, 0)) AS NB_NB_FREE_CLOPS_UNIQ,
    SUM(IFNULL(NB_NB_FREE_CLOPS_SESS, 0)) AS NB_NB_FREE_CLOPS_SESS,
    SUM(IFNULL(NB_NB_PAID_CLOPS_TOT, 0)) AS NB_NB_PAID_CLOPS_TOT,
    SUM(IFNULL(NB_NB_PAID_CLOPS_UNIQ, 0)) AS NB_NB_PAID_CLOPS_UNIQ,
    SUM(IFNULL(NB_NB_PAID_CLOPS_SESS, 0)) AS NB_NB_PAID_CLOPS_SESS,
    SUM(IFNULL(NB_NB_ALL_CLOPS_SESS, 0)) AS NB_NB_ALL_CLOPS_SESS,
    SUM(IFNULL(calls, 0)) AS calls,
    SUM(IFNULL(good_calls, 0)) AS good_calls,
    SUM(IFNULL(matched_calls, 0)) AS matched_calls,
    SUM(IFNULL(sold_calls, 0)) AS sold_calls,
    SUM(IFNULL(cite_gains, 0)) AS REVENUE,
    0 as CLICKS,
    0 as COSTS
FROM SESSIONS
LEFT JOIN {{ ref('EVENTS') }} AS EVENTS ON SESSIONS.session_id = EVENTS.dimension4
LEFT JOIN PAGE_VIEWS ON SESSIONS.session_id = PAGE_VIEWS.dimension4
LEFT JOIN CALL_GAINS USING (session_id)
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
),

COSTS AS (
SELECT
    Date AS Date,
    type as traffic_type,
    utm_source,
    utm_medium,
    IFNULL(utm_campaign, CampaignName) AS utm_campaign,
    IFNULL(utm_content, '') as utm_content,
    IFNULL(LOWER(utm_term), '') as utm_term,
    IFNULL(id_term, '') AS id_term,
    '' AS user_id,
    0 AS VISITS,
    0 AS VISITS_NB,
    0 AS NB_NB_FREE_CLOPS_TOT,
    0 AS NB_NB_FREE_CLOPS_UNIQ,
    0 AS NB_NB_FREE_CLOPS_SESS,
    0 AS NB_NB_PAID_CLOPS_TOT,
    0 AS NB_NB_PAID_CLOPS_UNIQ,
    0 AS NB_NB_PAID_CLOPS_SESS,
    0 AS NB_NB_ALL_CLOPS_SESS,
    0 AS calls,
    0 AS good_calls,
    0 AS matched_calls,
    0 AS sold_calls,
    0 AS REVENUE,
    SUM(Clicks) AS CLICKS,
    SUM(Cost * 1.2) AS COSTS
FROM {{ ref('ADS_ID_KEYWORD') }}
WHERE type = 'Новостройки'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
),

VISITKA_GAINS AS(
SELECT
    date,
    'Визитка' as traffic_type,
    'Визитка' as utm_source,
    'cpc' as utm_medium,
    campaign as utm_campaign,
    'Визитка' as utm_content,
    'Визитка' as utm_keyword,
    '' as keyword_id,
    '' as user_id,
    0 as VISITS,
    0 as VISITS_NB,
    0 AS NB_NB_FREE_CLOPS_TOT,
    0 AS NB_NB_FREE_CLOPS_UNIQ,
    0 AS NB_NB_FREE_CLOPS_SESS,
    0 AS NB_NB_PAID_CLOPS_TOT,
    0 AS NB_NB_PAID_CLOPS_UNIQ,
    0 AS NB_NB_PAID_CLOPS_SESS,
    0 AS NB_NB_ALL_CLOPS_SESS,
    IFNULL(SUM(calls), 0) as CALLS,
    IFNULL(SUM(good_calls), 0) as GOOD_CALLS,
    IFNULL(SUM(matched_calls), 0) as MATCHED_CALLS,
    IFNULL(SUM(sold_calls),0) as SOLD_CALLS,
    IFNULL(SUM(cite_gains), 0) as REVENUE,
    0 as CLICKS,
    0 as COSTS
FROM {{ source('CALLTOUCH_ATRIBUTION', 'VISITKA_GAINS_TABLES') }}
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
)

SELECT * FROM TRAFFIC_TABLE
UNION ALL
SELECT * FROM COSTS
UNION ALL
SELECT * FROM VISITKA_GAINS