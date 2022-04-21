WITH
SESSIONS AS (
SELECT
    date,
    user_id,
    session_id,
    utm_source,
    utm_medium,
    utm_campaign,
    deviceCategory,

    CASE
        WHEN REGEXP_CONTAINS(landingpage, 'moskva-i-oblast|moskva|moskovskaya-oblast') THEN 'MSK'
        WHEN REGEXP_CONTAINS(landingpage, 'sankt-peterburg|leningradskaya-oblast|sankt-peterburg-i-oblast') THEN 'SPB'
        ELSE ''
    END as city,

    CASE
        WHEN utm_campaign LIKE '%second-building%' THEN 'Классифайд'
        WHEN utm_campaign LIKE '%new-building%' THEN 'Новостройки'
        WHEN utm_campaign LIKE '%b2b%' THEN 'B2B'
        WHEN utm_campaign LIKE '%remont%' THEN 'Ремонт'
        WHEN utm_campaign LIKE '%app%' THEN 'App'
        ELSE traffic_type
    END AS traffic_type,

    traffic_type  as landing_type,

    CASE
        WHEN utm_medium = 'organic' AND landingpage = '/' THEN 'Брендовый поисковый'
        WHEN utm_medium = '(none)' AND landingpage = '/' THEN 'Брендовый прямой'
        WHEN utm_medium = 'organic' THEN 'Поисковый'
        WHEN utm_medium = 'olv' THEN 'olv'
        WHEN utm_source LIKE '%sea' THEN 'sea'
        WHEN utm_source LIKE 'advert%' THEN 'advert'
        WHEN utm_medium = 'display' OR REGEXP_CONTAINS(utm_campaign, 'poisk|kms_trafic|(video.*traffic)')
            THEN 'Медийный'
        WHEN utm_medium = 'cpc' OR (utm_medium = 'referral' and utm_source like 'vk%') THEN 'PPC Inhouse'
        ELSE 'Прочий'
    END AS RK_TRAFFIC

FROM {{ ref('SESSIONS') }} ),

TRAFFIC_TABLE AS (
SELECT
    date,
    user_id,
    deviceCategory,
    utm_source,
    utm_medium,
    IFNULL(utm_campaign, '') AS utm_campaign,
    city,
    traffic_type,
    landing_type,
    RK_TRAFFIC,
    COUNT(DISTINCT session_id) AS VISITS,
    SUM(IFNULL(CL_SB_FREE_CLOPS_TOT, 0)) AS CL_SB_FREE_CLOPS_TOT,
    SUM(IFNULL(CL_SB_FREE_CLOPS_UNIQ, 0)) AS CL_SB_FREE_CLOPS_UNIQ,
    SUM(IFNULL(CL_SB_FREE_CLOPS_SESS, 0)) AS CL_SB_FREE_CLOPS_SESS,
    SUM(IFNULL(CL_SB_VAS_CLOPS_TOT, 0)) AS CL_SB_VAS_CLOPS_TOT,
    SUM(IFNULL(CL_SB_VAS_CLOPS_UNIQ, 0)) AS CL_SB_VAS_CLOPS_UNIQ,
    SUM(IFNULL(CL_SB_VAS_CLOPS_SESS, 0)) AS CL_SB_VAS_CLOPS_SESS,
    SUM(IFNULL(CL_SB_ALL_CLOPS_SESS, 0)) AS CL_SB_ALL_CLOPS_SESS,
    SUM(IFNULL(CL_NB_FREE_CLOPS_TOT, 0)) AS CL_NB_FREE_CLOPS_TOT,
    SUM(IFNULL(CL_NB_FREE_CLOPS_UNIQ, 0)) AS CL_NB_FREE_CLOPS_UNIQ,
    SUM(IFNULL(CL_NB_FREE_CLOPS_SESS, 0)) AS CL_NB_FREE_CLOPS_SESS,
    SUM(IFNULL(CL_NB_PAID_CLOPS_TOT, 0)) AS CL_NB_PAID_CLOPS_TOT,
    SUM(IFNULL(CL_NB_PAID_CLOPS_UNIQ, 0)) AS CL_NB_PAID_CLOPS_UNIQ,
    SUM(IFNULL(CL_NB_PAID_CLOPS_SESS, 0)) AS CL_NB_PAID_CLOPS_SESS,
    SUM(IFNULL(P_AGENT_REG_SUCCESS_TOT, 0)) AS P_AGENT_REG_SUCCESS_TOT,
    SUM(IFNULL(P_AGENT_REG_SUCCESS_UNIQ, 0)) AS P_AGENT_REG_SUCCESS_UNIQ,
    SUM(IFNULL(P_AGENT_REG_SUCCESS_SESS, 0)) AS P_AGENT_REG_SUCCESS_SESS,
    SUM(IFNULL(IB_SESS, 0)) AS IB_SESS,
    0 AS Impressions,
    0 AS Clicks,
    0 AS Cost

FROM SESSIONS
LEFT JOIN {{ ref('EVENTS') }}  AS EVENTS ON SESSIONS.session_id = EVENTS.dimension4
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    ),

COSTS AS (
SELECT
    Date AS date,
    '' as user_id,
    '' as deviceCategory,
    utm_source,
    utm_medium,
    IFNULL(utm_campaign, CampaignName) AS utm_campaign,
    '' as city,
    IFNULL(type, '') as traffic_type,
    '' as landing_type,
    'PPC Inhouse' as RK_TRAFFIC,
    0 AS VISITS,
    0 AS CL_SB_FREE_CLOPS_TOT,
    0 AS CL_SB_FREE_CLOPS_UNIQ,
    0 AS CL_SB_FREE_CLOPS_SESS,
    0 AS CL_SB_VAS_CLOPS_TOT,
    0 AS CL_SB_VAS_CLOPS_UNIQ,
    0 AS CL_SB_VAS_CLOPS_SESS,
    0 AS CL_SB_ALL_CLOPS_SESS,
    0 AS CL_NB_FREE_CLOPS_TOT,
    0 AS CL_NB_FREE_CLOPS_UNIQ,
    0 AS CL_NB_FREE_CLOPS_SESS,
    0 AS CL_NB_PAID_CLOPS_TOT,
    0 AS CL_NB_PAID_CLOPS_UNIQ,
    0 AS CL_NB_PAID_CLOPS_SESS,
    0 AS P_AGENT_REG_SUCCESS_TOT,
    0 AS P_AGENT_REG_SUCCESS_UNIQ,
    0 AS P_AGENT_REG_SUCCESS_SESS,
    0 AS IB_SESS,
    SUM(Impressions) AS Impressions,
    SUM(Clicks) AS Clicks,
    SUM(Cost * 1.2) AS Cost
FROM
    {{ ref('ADS') }} 
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
),

UNION_TABLE AS (
    SELECT * FROM TRAFFIC_TABLE
    UNION ALL
    SELECT * FROM COSTS
)

SELECT * EXCEPT (city),
    CASE
        WHEN utm_campaign like '%msk%' THEN 'MSK'
        WHEN utm_campaign like '%spb%' THEN 'SPB'
        WHEN utm_campaign like '%rus%' THEN 'OTHER'
        WHEN city <> '' THEN city
        ELSE 'OTHER'
    END as CITY,
FROM UNION_TABLE