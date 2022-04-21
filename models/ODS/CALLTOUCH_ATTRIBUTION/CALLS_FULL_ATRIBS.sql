WITH NB_CITE_CALLS AS (
SELECT 
 date_time,
 caller,
 SOLDNESS,
 SOLD_SUM
FROM {{ ref('NB_CALLS') }}
WHERE type = 'САЙТ')
SELECT
  IFNULL(last_session , 'Неопределены')                           as session_id,
  IFNULL(first_session , 'Неопределены')                          as session_id_first,
  IFNULL(last_non_direct_visit , 'Неопределены')                  as session_id_last_nondirect,
  first_call_dt,
  callerNumber                                                    as callerNumber,
  CASE WHEN first_call_dt  < date_time + INTERVAL 180 MINUTE THEN 1 ELSE 0 END as has_sold,
  SUM(calls)                                                      as calls,
  SUM(good_calls)                                                 as good_calls,
  IFNULL(COUNT(caller),0)                                         as matched_calls,
  IFNULL(SUM(CASE WHEN SOLDNESS = 'Продан' THEN 1 ELSE 0 END),0)  as sold_calls,
  IFNULL(SUM(SOLD_SUM),0)                                         as cite_gains
FROM
  {{ ref('CALLTOUCH_FULL_ATTRIBUTION') }} as CT_CALLS
LEFT JOIN
NB_CITE_CALLS AS CALL
  ON CT_CALLS.callerNumber = CALL.caller
GROUP BY 1,2,3,4,5,6