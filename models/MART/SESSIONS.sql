(
    WITH
  users_bots AS (
  SELECT
    dimension1,
    dimension2
  FROM
    {{ source('UA_REPORTS', 'USERS') }}
  GROUP BY
    1,
    2 )
    , ids_drop as (
SELECT
  dimension1,
  id AS dimension2
FROM
  {{ source('ARCHIVE', 'BOT_IDS') }} AS ids
LEFT JOIN
  users_bots as us_table
ON
  id = dimension2
where dimension2 is not null
  )


SELECT *
FROM (
         SELECT EXTRACT(date from dateHourMinute)                                       as date,
                dateHourMinute                                                          AS session_timestamp,
                dimension1                                                              AS user_id,
                traff_details.dimension4                                                AS session_id,
                CASE
                    WHEN landingpagepath LIKE '%novostroyki%' THEN 'Новостройки'
                    WHEN landingpagepath LIKE '%nedvizhimost%' THEN 'Классифайд'
                    WHEN landingpagepath LIKE '%remont%' THEN 'Ремонт'
                    WHEN landingpagepath LIKE '%services/deal%'
                        OR landingpagepath LIKE '%online-deal%' THEN 'Покупка Онлайн'
                    WHEN landingpagepath LIKE '%guaranteed-deal%' THEN 'Защита сделки'
                    WHEN landingpagepath LIKE '%ipoteka%' THEN 'Ипотека'
                    WHEN landingpagepath LIKE '%rieltor%' THEN 'B2B'
                    WHEN landingpagepath = '/' THEN 'Главная'
                    WHEN landingpagepath LIKE '%proverka-yuridicheskoy-chistoty-kvartiry%' THEN 'Проверка недвижимости'
                    WHEN landingpagepath LIKE '%eregistration%' THEN 'ЭЛ.РЕГ'
                    WHEN landingpagepath LIKE '%personal-area%' THEN 'Личный кабинет'
                    WHEN landingpagepath LIKE '%/news/%' THEN 'Новости'
                    WHEN landingpagepath = '(not set)' THEN 'Неопределено'
                    ELSE
                        'Другие'
                    END                                                                 AS traffic_type,
                landingpagepath                                                         AS landingpage,

                CASE
                    WHEN medium in ('referral', 'organic') and (source like 'yandex.%' or source = 'ya.ru')
                        THEN 'yandex'
                    ELSE
                        source END
                                                                                        as utm_source,
                CASE
                    WHEN medium = 'Email' THEN 'email'
                    WHEN medium = 'referral' and source like '%yandex%' THEN 'organic'
                    ELSE medium END                                                     as utm_medium,
                CASE
                    WHEN campaign LIKE "%\\%25%"
                        THEN REPLACE(REPLACE(replace(campaign, '25', ''), '%3A', ':'), '%', '')
                    ELSE campaign
                    END                                                                 as utm_campaign,

                CASE
                    WHEN adContent LIKE "%adid%"
                        THEN REPLACE(REGEXP_EXTRACT(adContent, r'adid:[0-9]+'), 'adid:', '')
                    ELSE adContent
                    END                                                                 as utm_content,

                keyword                                                                 as utm_keyword,
                
                CASE
                    WHEN adContent LIKE "%phrasid%" and keyword NOT IN ('---autotargeting', '(not set)')
                        THEN IFNULL(REPLACE(REGEXP_EXTRACT(adContent, r'phrasid:(kwd-[0-9]+|[0-9]+)'), 'kwd-', ''), '')
                    ELSE ""
                    END                                                                 as keyword_id,
                deviceCategory,
                ROW_NUMBER() OVER (PARTITION BY dimension4 ORDER BY dateHourMinute ASC) as first,
                CASE
                    WHEN source = '(direct)' and dimension4 in (SELECT distinct dimension4
                                                                FROM {{ source('UA_REPORTS', 'VISIT_QUALITY') }}
                                                                where sessionDuration < 27)
                        THEN 1
                    ELSE 0 END
                                                                                        as bots
         FROM {{ source('UA_REPORTS', 'UA_TRAFIC_BIG') }} AS traff_details

         )
WHERE first = 1
  and bots = 0
  and user_id  not in (select distinct dimension1 from  ids_drop)  
    )