WITH
    AdBasicStats AS (
        SELECT * FROM {{ source('Google_Ads', 'AdBasicStats_4291182672') }}
        UNION ALL
        SELECT * FROM {{ source('Google_Ads', 'AdBasicStats_6169350493') }}
        UNION ALL
        SELECT * FROM {{ source('Google_Ads', 'AdBasicStats_6116221976') }}
        UNION ALL
        SELECT * FROM {{ source('Google_Ads', 'AdBasicStats_6781394611') }}
    ),
    CampaignBasicStats AS (
        SELECT * FROM {{ source('Google_Ads', 'CampaignBasicStats_4291182672') }}
        UNION ALL
        SELECT * FROM {{ source('Google_Ads', 'CampaignBasicStats_6169350493') }}
        UNION ALL
        SELECT * FROM {{ source('Google_Ads', 'CampaignBasicStats_6116221976') }}
        UNION ALL
        SELECT * FROM {{ source('Google_Ads', 'CampaignBasicStats_6781394611') }}
    ),
    Campaign AS (
        SELECT * FROM {{ source('Google_Ads', 'Campaign_4291182672') }}
        UNION ALL
        SELECT * FROM {{ source('Google_Ads', 'Campaign_6169350493') }}
        UNION ALL
        SELECT * FROM {{ source('Google_Ads', 'Campaign_6116221976') }}
        UNION ALL
        SELECT * FROM {{ source('Google_Ads', 'Campaign_6781394611') }}
    ),
    Keyword AS (
        SELECT * FROM {{ source('Google_Ads', 'Keyword_4291182672') }}
        UNION ALL
        SELECT * FROM {{ source('Google_Ads', 'Keyword_6169350493') }}
        UNION ALL
        SELECT * FROM {{ source('Google_Ads', 'Keyword_6116221976') }}
        UNION ALL
        SELECT * FROM {{ source('Google_Ads', 'Keyword_6781394611') }}
    ),
    BasicStats AS (
        SELECT
            'Ad' AS Level,
            Date,
            ExternalCustomerId,
            CampaignId,
            AdGroupId,
            CriterionId,
            CreativeId,
            Impressions,
            Clicks,
            Cost
        FROM AdBasicStats
        UNION ALL
        SELECT
            'Campaign' AS Level,
            Date,
            ExternalCustomerId,
            CampaignId,
            0 AS AdGroupId,
            0 AS CriterionId,
            0 AS CreativeId,
            Impressions,
            Clicks,
            Cost
        FROM CampaignBasicStats
    ),
    Report AS (
        SELECT
            CASE A.ExternalCustomerId
                WHEN 4291182672 THEN 'Классифайд'
                WHEN 6169350493 THEN 'Новостройки'
                WHEN 6116221976 THEN 'B2B'
                WHEN 6781394611 THEN 'App'
            END as type,
            A.Date,
            C.CampaignName,
            IFNULL(REGEXP_EXTRACT(C.TrackingUrlTemplate, r'^.*?=(.*?)&.*?'), 'google')     AS utm_source,
            IFNULL(REGEXP_EXTRACT(C.TrackingUrlTemplate, r'^.*?=.*?=.*?(.*?)&'), 'cpc')    AS utm_medium,
            IF(A.ExternalCustomerId = 6781394611, C.CampaignName,
                CONCAT(REGEXP_EXTRACT(C.TrackingUrlTemplate, r'^.*?=.*?=.*?=(.*?):'), ':', C.CampaignId))
                                                                                            AS utm_campaign,
            REGEXP_REPLACE(CAST(A.CreativeId AS STRING), '^0$', '')                         AS utm_content,
            CASE
                WHEN A.ExternalCustomerId = 6781394611 THEN 'Google_app_cmp_target'
                WHEN K.Criteria IN ('AutomaticContent', 'Content') OR K.Criteria IS NULL THEN '(not set)'
                ELSE K.Criteria
            END                                                                             AS utm_keyword,
            IF(K.CriterionId = 3000006, null, CAST(K.CriterionId AS STRING))                AS id_keyword,
            SUM(A.Impressions)                                                              AS Impressions,
            SUM(A.Clicks)                                                                   AS Clicks,
            ROUND(SUM(A.Cost / 1000000), 2)                                                 AS Cost
        FROM BasicStats AS A
        LEFT JOIN Campaign AS C ON A.ExternalCustomerId = C.ExternalCustomerId
                                  AND A.CampaignId = C.CampaignId
                                  AND A.Date = C._DATA_DATE
        LEFT JOIN Keyword AS K ON A.ExternalCustomerId = K.ExternalCustomerId
                                  AND A.AdGroupId = K.AdGroupId
                                  AND A.CriterionId = K.CriterionId
                                  AND A.Date = K._DATA_DATE
        WHERE (A.Level = 'Ad' AND C.AdvertisingChannelType IS NOT NULL ) OR
              (A.Level = 'Campaign' AND (C.AdvertisingChannelType IS NULL OR A.ExternalCustomerId = 6781394611))
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
    )

SELECT * FROM {{ source('EXTERNAL_DATA_SOURCES', 'GOOGLE_ADS_legacy') }}
UNION ALL
SELECT * FROM Report
WHERE Date > (SELECT MAX(Date) FROM {{ source('EXTERNAL_DATA_SOURCES', 'GOOGLE_ADS_legacy') }})
ORDER BY Date