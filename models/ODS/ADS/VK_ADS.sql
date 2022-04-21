SELECT
  CASE
    WHEN adAccount = '1606924159' THEN 'Новостройки'
    WHEN adAccount = '1606924161' THEN 'Классифайд'
END
  AS type,
  date,
  CampaignName,
  source AS utm_source,
  medium AS utm_medium,
  campaign AS utm_campaign,
  adContent AS utm_content,
  keyword AS utm_term,
  '' AS id_keyword,
  SUM(Impressions) AS Impressions,
  SUM(adClicks) AS Clicks,
  SUM(adCost) AS Cost
FROM {{ source('OWOX_ADS', 'vk_OWOXAdCostData') }}
--WHERE date > (SELECT MAX(Date) FROM `m2-main.ADS_COST.VK_ADS`) 
group by 1,2,3,4,5,6,7,8,9