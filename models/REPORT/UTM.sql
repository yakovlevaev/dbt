SELECT
  date,
  utm_source,
  utm_medium,
  utm_campaign,
  utm_content,
  utm_keyword,
  COUNT(DISTINCT session_id) AS sessions,
  IFNULL(SUM(ALL_REG_SUCCESS_UNIQ), 0) AS REG_SUCCESS_UNIQ
FROM {{ ref('SESSIONS') }}
LEFT JOIN {{ ref('EVENTS') }} ON dimension4 = session_id
GROUP BY 1, 2, 3, 4, 5, 6