SELECT
  CASE
    WHEN adAccount = '8ee847846c@agency_client' THEN 'Новостройки'
    WHEN adAccount = '9644e23da9@agency_client' THEN 'Классифайд'
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
  SUM(originalAdCost) AS Cost
FROM {{ source('OWOX_ADS', 'my_target_OWOXAdCostData') }}
--WHERE date > (select max(Date) FROM `m2-main.ADS_COST.MT_ADS`) 
group by 1,2,3,4,5,6,7,8,9