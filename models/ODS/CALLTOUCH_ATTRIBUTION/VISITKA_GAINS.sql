with visit_calls as (SELECT
  extract(date from date_time_msk) as date,
  date_time_msk,
  CASE when utmCampaign  like '%Визит%' then utmCampaign ELSE clientId END AS visitk,
  callerNumber,
  timestamp,
  CASE WHEN successful THEN 1 ELSE 0 END as  successful,
  CASE WHEN uniqTargetCall THEN 1 ELSE 0 END as uniq_target,
FROM {{ source('EXTERNAL_DATA_SOURCES', 'CALLTOUCH_JOURNAL') }}
WHERE  utmCampaign  like '%Визит%'
order by date_time_msk),
 NB_CITE_CALLS AS (
SELECT
 extract(date from date_time) as date,
 caller,
 SOLDNESS,
 min(date_time) as date_time,
 sum(SOLD_SUM) as SOLD_SUM
FROM {{ ref('NB_CALLS') }}
WHERE type = 'САЙТ'
GROUP BY 1,2,3), VISIT_CALL_AGG_TABLE AS (
SELECT
  visitk                            as session_id,
  callerNumber                                                    as callerNumber,
  CT_CALLS.date as date,
  date_time_msk,
  CASE WHEN date_time_msk  < date_time + INTERVAL 180 MINUTE THEN 1 ELSE 0 END as has_sold,
  COUNT( DISTINCT callerNumber)                                   as calls,
  SUM(successful)                                                 as good_calls,
  IFNULL(COUNT(caller),0)                                         as matched_calls,
  IFNULL(SUM(CASE WHEN SOLDNESS = 'Продан' THEN 1 ELSE 0 END),0)  as sold_calls,
  IFNULL(SUM(SOLD_SUM),0)                                         as cite_gains
FROM
  visit_calls as CT_CALLS
LEFT JOIN
NB_CITE_CALLS AS CALL
  ON CT_CALLS.callerNumber = CALL.caller
  and 
  CT_CALLS.date = CALL.date
GROUP BY 1,2,3,4,5)

SELECT 
 date,
 'Визитка' as source,
 'Визитка' as medium,
 session_id       as campaign,
 sum(has_sold)    as has_sold,
 sum(calls)       as calls,
 sum(good_calls)  as good_calls,
 sum(matched_calls)  as matched_calls,
 sum(sold_calls)  as sold_calls,
 sum(cite_gains)  as cite_gains
FROM VISIT_CALL_AGG_TABLE
GROUP BY 1,2,3,4