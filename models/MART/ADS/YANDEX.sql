SELECT CASE
           WHEN AdAccount = 'm2advteam' THEN 'Новостройки'
           WHEN AdAccount = 'm2advteamsecondb' THEN 'Классифайд'
           WHEN AdAccount = 'm2advremont' THEN 'Ремонт'
           WHEN AdAccount = 'm2advteamb2b' THEN 'B2B'
           ELSE "WTF" END AS type,
       Date,
       CampaignName,
       utm.utm_source     AS utm_source,
       utm.utm_medium     AS utm_medium,
       IF(utm.utm_campaign = '',  CONCAT(CampaignName, ':', CampaignId), utm.utm_campaign) AS utm_campaign,
       REPLACE (REGEXP_EXTRACT(utm.utm_content, r'adid:[0-9]+'), 'adid:', '') as utm_content,
       CASE 
            WHEN Criterion = '---autotargeting' THEN Criterion
            WHEN utm.utm_term = '' THEN '(not set)'
            ELSE utm.utm_term
       END AS utm_term,
       IFNULL(REPLACE(REGEXP_EXTRACT(utm.utm_content, r'phrasid:[0-9]+'), 'phrasid:', ''), '') AS id_term,
       SUM(Impressions)   AS Impressions,
       SUM(Clicks)        AS Clicks,
       SUM(Cost)          AS Cost
FROM {{ source('OWOX_ADS', 'yandex_direct_AdCostData') }}
WHERE Impressions > 0 OR Clicks > 0 OR Cost > 0
--WHERE date > (SELECT MAX(date) AS date FROM `m2-main.ADS_COST.YDX_ADS`)
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9

UNION ALL
SELECT * FROM {{ source('EXTERNAL_DATA_SOURCES', 'YDX_ADS_legacy') }} 