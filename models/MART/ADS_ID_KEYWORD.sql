WITH ALL_ADS AS ( 
SELECT * FROM {{ ref('FB_ADS') }}
UNION ALL 
SELECT * FROM {{ ref('GOOGLE_ADS') }}
UNION ALL 
SELECT * FROM {{ ref('MT_ADS') }}
UNION ALL 
SELECT * FROM {{ ref('VK_ADS') }}
UNION ALL 
SELECT * FROM {{ ref('YDX_ADS') }}
)

SELECT 
type, 
Date,
CampaignName, 
utm_source,
utm_medium,
utm_campaign,
CASE 
  WHEN utm_content LIKE "%adid%" THEN REPLACE(REGEXP_EXTRACT(utm_content, r'adid:[0-9]+'), 'adid:' ,'') 
  ELSE utm_content 
END as utm_content,
utm_term,
id_keyword as id_term,
SUM(Impressions) as Impressions,
SUM(Clicks) as Clicks ,
SUM(Cost) as Cost
FROM ALL_ADS
GROUP BY 
1,2,3,4,5,6,7,8,9