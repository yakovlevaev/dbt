WITH
  CT_USERS AS (
  SELECT
    clientId,
    callerNumber,
    MIN(date_time_msk) AS first_call_dt,
    SUM(CASE
        WHEN successful THEN 1
      ELSE
      0
    END
      ) AS calls,
    SUM(CASE
        WHEN uniqTargetCall THEN 1
      ELSE
      0
    END
      ) AS good_calls
  FROM {{ source('EXTERNAL_DATA_SOURCES', 'CALLTOUCH_JOURNAL') }}
  WHERE
    clientId IS NOT NULL
  GROUP BY
    1,
    2 ),
  ATRIB_TABLE_DUBL AS (
  SELECT
    user_id,
    session_timestamp,
    session_id,
    utm_source,
    first_call_dt
  FROM {{ ref('SESSIONS') }}
  INNER JOIN
    CT_USERS
  ON
    CT_USERS.clientId = user_id),
  NON_DIRECT_USERS AS (
  SELECT
    user_id,
    session_id,
    session_timestamp,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY session_timestamp DESC) AS visit_dt
  FROM
    ATRIB_TABLE_DUBL
  WHERE
    session_timestamp <= first_call_dt
    AND utm_source != '(direct)'),
  DIRECT_USERS AS (
  SELECT
    user_id,
    session_id,
    session_timestamp,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY session_timestamp DESC) AS visit_dt
  FROM
    ATRIB_TABLE_DUBL
  WHERE
    user_id NOT IN (
    SELECT
      DISTINCT user_id AS user_id
    FROM
      ATRIB_TABLE_DUBL
    WHERE
      session_timestamp <= first_call_dt
      AND utm_source != '(direct)')
    AND session_timestamp <= first_call_dt ),
  LAST_NON_DIRECT AS (
  SELECT
    user_id,
    session_id as last_non_direct_visit
  FROM (
    SELECT
      user_id,
      session_id
    FROM
      NON_DIRECT_USERS
    WHERE
      visit_dt = 1
    UNION ALL
    SELECT
      user_id,
      session_id
    FROM
      DIRECT_USERS
    WHERE
      visit_dt = 1)),
  ALL_USERS AS (
  SELECT
    user_id,
    session_id,
    session_timestamp,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY session_timestamp) AS FIRST_visit_dt,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY session_timestamp DESC) AS LAST_visit_dt
  FROM
    ATRIB_TABLE_DUBL
  WHERE
    session_timestamp <= first_call_dt ),
  SIMPLE_LAST AS (
  SELECT
    user_id,
    session_id as last_session
  FROM
    ALL_USERS
  WHERE
    LAST_visit_dt =1),

  SIMPLE_FRIST AS (
  SELECT
    user_id,
    session_id as first_session
  FROM
    ALL_USERS
  WHERE
    FIRST_visit_dt =1)

SELECT
  clientId as user_id,
  callerNumber,
  calls,
  good_calls,
  first_call_dt,
  first_session,
  last_session,
  last_non_direct_visit
FROM
  CT_USERS
LEFT JOIN
  SIMPLE_FRIST
ON
  CT_USERS.clientId = SIMPLE_FRIST.user_id
LEFT JOIN
  SIMPLE_LAST
ON
  CT_USERS.clientId = SIMPLE_LAST.user_id

LEFT JOIN
  LAST_NON_DIRECT
ON
  CT_USERS.clientId = LAST_NON_DIRECT.user_id
WHERE first_session is not null