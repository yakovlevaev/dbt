SELECT
  CASE
    WHEN ad_account_name LIKE '%Новостройки%' THEN 'Новостройки'
    WHEN ad_account_name LIKE '%Ремонт%' THEN 'Ремонт'
    WHEN ad_account_name LIKE '%Вторичное%' THEN 'Классифайд'
END
  AS type,
  Date,
  campaign.name AS CampaignName,
  utm.utm_source AS utm_source,
  utm.utm_medium AS utm_medium,
  utm.utm_campaign AS utm_campaign,
  utm.utm_content AS utm_content,
  utm.utm_term AS utm_term,
  '' AS id_keyword,
  SUM(impressions) AS Impressions,
  SUM(clicks) AS Clicks,
  ROUND(SUM(spend), 2) AS Cost
FROM {{ source('OWOX_ADS', 'facebook_AdCostData') }}
--WHERE date > (SELECT MAX(date) as date FROM `m2-main.ADS_COST.FB_ADS`) 
group by 1,2,3,4,5,6,7,8,9