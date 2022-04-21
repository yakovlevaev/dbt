WITH
CL_CLOPS_1 AS (
SELECT
    dimension4,
    sum(CASE WHEN REGEXP_CONTAINS(eventlabel, 'ClCardSellPhoneClickBottom|ClCardSellPhoneClickTop|ClSerpSellPhoneClick') then totalEvents ELSE 0 END) as CL_SB_FREE_CLOPS_TOT,
    sum(CASE WHEN REGEXP_CONTAINS(eventlabel, 'ClCardSellPhoneClickBottom|ClCardSellPhoneClickTop|ClSerpSellPhoneClick') then uniqueEvents ELSE 0 END) as CL_SB_FREE_CLOPS_UNIQ,
    max(CASE WHEN REGEXP_CONTAINS(eventlabel, 'ClCardSellPhoneClickBottom|ClCardSellPhoneClickTop|ClSerpSellPhoneClick') then 1 ELSE 0 END) as CL_SB_FREE_CLOPS_SESS,

    sum(CASE WHEN REGEXP_CONTAINS(eventlabel, 'ClCardSellSecondVasPhoneClickAll|ClSerpSellSecondVasPhoneClick') then totalEvents ELSE 0 END) as CL_SB_VAS_CLOPS_TOT,
    sum(CASE WHEN REGEXP_CONTAINS(eventlabel, 'ClCardSellSecondVasPhoneClickAll|ClSerpSellSecondVasPhoneClick') then uniqueEvents ELSE 0 END) as CL_SB_VAS_CLOPS_UNIQ,
    max(CASE WHEN REGEXP_CONTAINS(eventlabel, 'ClCardSellSecondVasPhoneClickAll|ClSerpSellSecondVasPhoneClick') then 1 ELSE 0 END) as CL_SB_VAS_CLOPS_SESS,

    max(CASE WHEN REGEXP_CONTAINS(eventlabel, 
        'ClCardSellPhoneClickBottom|ClCardSellPhoneClickTop|ClSerpSellPhoneClick|ClCardSellSecondVasPhoneClickAll|ClSerpSellSecondVasPhoneClick') then 1 ELSE 0 END) 
        as CL_SB_ALL_CLOPS_SESS,

    0 as CL_NB_FREE_CLOPS_TOT,
    0 as CL_NB_FREE_CLOPS_UNIQ,
    0 as CL_NB_FREE_CLOPS_SESS,

    0 as CL_NB_PAID_CLOPS_TOT,
    0 as CL_NB_PAID_CLOPS_UNIQ,
    0 as CL_NB_PAID_CLOPS_SESS

FROM {{ source('UA_REPORTS', 'RAW_EVENTS') }} 
WHERE dateHourMinute < '2022-01-26'
GROUP BY 1
),

CL_CLOPS_2 AS (
SELECT
    dimension4,
    sum(CASE WHEN eventlabel = 'UPSecondSaleClopsFree' then totalEvents ELSE 0 END) as CL_SB_FREE_CLOPS_TOT,
    sum(CASE WHEN eventlabel = 'UPSecondSaleClopsFree' then uniqueEvents ELSE 0 END) as CL_SB_FREE_CLOPS_UNIQ,
    max(CASE WHEN eventlabel = 'UPSecondSaleClopsFree' then 1 ELSE 0 END) as CL_SB_FREE_CLOPS_SESS,

    sum(CASE WHEN eventlabel = 'UPSecondSaleClopsVAS' then totalEvents ELSE 0 END) as CL_SB_VAS_CLOPS_TOT,
    sum(CASE WHEN eventlabel = 'UPSecondSaleClopsVAS' then uniqueEvents ELSE 0 END) as CL_SB_VAS_CLOPS_UNIQ,
    max(CASE WHEN eventlabel = 'UPSecondSaleClopsVAS' then 1 ELSE 0 END) as CL_SB_VAS_CLOPS_SESS,

    max(CASE WHEN eventlabel IN ('UPSecondSaleClopsFree', 'UPSecondSaleClopsVAS') then 1 ELSE 0 END) as CL_SB_ALL_CLOPS_SESS,

    sum(CASE WHEN eventlabel like '%New%PhoneClick%' then totalEvents ELSE 0 END) as CL_NB_FREE_CLOPS_TOT,
    sum(CASE WHEN eventlabel like '%New%PhoneClick%' then uniqueEvents ELSE 0 END) as CL_NB_FREE_CLOPS_UNIQ,
    max(CASE WHEN eventlabel like '%New%PhoneClick%' then 1 ELSE 0 END) as CL_NB_FREE_CLOPS_SESS,

    sum(CASE WHEN eventlabel like '%ClSerpCardSellNewDevPaidPhoneClick%' then totalEvents ELSE 0 END) as CL_NB_PAID_CLOPS_TOT,
    sum(CASE WHEN eventlabel like '%ClSerpCardSellNewDevPaidPhoneClick%' then uniqueEvents ELSE 0 END) as CL_NB_PAID_CLOPS_UNIQ,
    max(CASE WHEN eventlabel like '%ClSerpCardSellNewDevPaidPhoneClick%' then 1 ELSE 0 END) as CL_NB_PAID_CLOPS_SESS

FROM {{ source('UA_REPORTS', 'RAW_EVENTS') }} 
WHERE dateHourMinute between '2022-01-26' and '2022-02-16'
GROUP BY 1
),

CL_CLOPS_3 AS (
SELECT
    dimension4,
    sum(CASE WHEN eventCategory like 'second_sell_false%' then totalEvents ELSE 0 END) as CL_SB_FREE_CLOPS_TOT,
    sum(CASE WHEN eventCategory like 'second_sell_false%' then uniqueEvents ELSE 0 END) as CL_SB_FREE_CLOPS_UNIQ,
    max(CASE WHEN eventCategory like 'second_sell_false%' then 1 ELSE 0 END) as CL_SB_FREE_CLOPS_SESS,

    sum(CASE WHEN eventCategory like 'second_sell_true%' then totalEvents ELSE 0 END) as CL_SB_VAS_CLOPS_TOT,
    sum(CASE WHEN eventCategory like 'second_sell_true%' then uniqueEvents ELSE 0 END) as CL_SB_VAS_CLOPS_UNIQ,
    max(CASE WHEN eventCategory like 'second_sell_true%' then 1 ELSE 0 END) as CL_SB_VAS_CLOPS_SESS,

    max(CASE WHEN eventCategory like 'second_sell%' then 1 ELSE 0 END) as CL_SB_ALL_CLOPS_SESS,

    sum(CASE WHEN eventCategory like 'new_dev_sell%unknown' then totalEvents ELSE 0 END) as CL_NB_FREE_CLOPS_TOT, 
    sum(CASE WHEN eventCategory like 'new_dev_sell%unknown' then uniqueEvents ELSE 0 END) as CL_NB_FREE_CLOPS_UNIQ,
    max(CASE WHEN eventCategory like 'new_dev_sell%unknown' then 1 ELSE 0 END) as CL_NB_FREE_CLOPS_SESS,

    sum(CASE WHEN eventCategory like 'new_dev_sell%' and eventCategory not like '%unknown' then totalEvents ELSE 0 END) as CL_NB_PAID_CLOPS_TOT,
    sum(CASE WHEN eventCategory like 'new_dev_sell%' and eventCategory not like '%unknown' then uniqueEvents ELSE 0 END) as CL_NB_PAID_CLOPS_UNIQ,
    max(CASE WHEN eventCategory like 'new_dev_sell%' and eventCategory not like '%unknown' then 1 ELSE 0 END) as CL_NB_PAID_CLOPS_SESS

FROM {{ source('UA_REPORTS', 'RAW_EVENTS') }} 
WHERE dateHourMinute > '2022-02-16'
GROUP BY 1
),

CL_CLOPS AS (
SELECT * FROM CL_CLOPS_1
UNION ALL 
SELECT * FROM CL_CLOPS_2
UNION ALL
SELECT * FROM CL_CLOPS_3
),

NB_CLOPS AS (
SELECT
    dimension4,
    SUM(CASE WHEN eventCategory IN ('Nb Unknown', 'Nb') THEN totalEvents ELSE 0 END) AS NB_NB_FREE_CLOPS_TOT,
    SUM(CASE WHEN eventCategory IN ('Nb Unknown', 'Nb') THEN uniqueEvents ELSE 0 END) AS NB_NB_FREE_CLOPS_UNIQ,
    MAX(CASE WHEN eventCategory IN ('Nb Unknown', 'Nb') THEN 1 ELSE 0 END) AS NB_NB_FREE_CLOPS_SESS,

    SUM(CASE WHEN eventCategory NOT IN ('Nb Unknown', 'Nb') THEN totalEvents ELSE 0 END) AS NB_NB_PAID_CLOPS_TOT,
    SUM(CASE WHEN eventCategory NOT IN ('Nb Unknown', 'Nb') THEN uniqueEvents ELSE 0 END) AS NB_NB_PAID_CLOPS_UNIQ,
    MAX(CASE WHEN eventCategory NOT IN ('Nb Unknown', 'Nb') THEN 1 ELSE 0 END) AS NB_NB_PAID_CLOPS_SESS,

    1 AS NB_NB_ALL_CLOPS_SESS

FROM {{ source('UA_REPORTS', 'RAW_EVENTS') }} 
WHERE REGEXP_CONTAINS(eventlabel, 'NbSnippetPhoneNumberClick|NbCardPhoneСallRequestSendClick|NbCardPhoneNumberClick')
GROUP BY 1
),

REG_SUCCESS AS (
SELECT
    dimension4,
    
    sum(totalEvents) as ALL_REG_SUCCESS_TOT,
    sum(uniqueEvents) as ALL_REG_SUCCESS_UNIQ,
    1 as ALL_REG_SUCCESS_SESS,

    sum(CASE WHEN eventAction = 'FL' then totalEvents ELSE 0 END) as FL_REG_SUCCESS_TOT,
    sum(CASE WHEN eventAction = 'FL' then uniqueEvents ELSE 0 END) as FL_REG_SUCCESS_UNIQ,
    max(CASE WHEN eventAction = 'FL' then 1 ELSE 0 END) as FL_REG_SUCCESS_SESS,

    sum(CASE WHEN eventAction = 'P_AGENT' then totalEvents ELSE 0 END) as P_AGENT_REG_SUCCESS_TOT,
    sum(CASE WHEN eventAction = 'P_AGENT' then uniqueEvents ELSE 0 END) as P_AGENT_REG_SUCCESS_UNIQ,
    max(CASE WHEN eventAction = 'P_AGENT' then 1 ELSE 0 END) as P_AGENT_REG_SUCCESS_SESS,

    sum(CASE WHEN eventAction = 'PARTNER' then totalEvents ELSE 0 END) as PARTNER_REG_SUCCESS_TOT,
    sum(CASE WHEN eventAction = 'PARTNER' then uniqueEvents ELSE 0 END) as PARTNER_REG_SUCCESS_UNIQ,
    max(CASE WHEN eventAction = 'PARTNER' then 1 ELSE 0 END) as PARTNER_REG_SUCCESS_SESS,

FROM {{ source('UA_REPORTS', 'RAW_EVENTS') }} 
WHERE eventlabel = 'NewRegSuccess' and eventCategory = 'Registration'
GROUP BY 1
),
     
OTHERS AS (
SELECT
    dimension4,
    MAX(CASE
      WHEN REGEXP_CONTAINS(eventlabel, 'CardNBBannerRight|NbCardIBSendBankCallBack|NbCardVariantMonthMortgageClick|ClCardSellMortgageCalcClick') THEN 1
      WHEN REGEXP_CONTAINS(eventlabel, 'MoreButtonClick|CalcButtonClick')
        AND eventCategory = 'MainPage'AND eventAction IN ('HomePromoBlockMortgageTab','HomeMortgageBlock') THEN 1
      WHEN REGEXP_CONTAINS(eventlabel, '_ipoteka_call_back|_ipoteka_calc_click|_ipoteka_banks') THEN 1
    ELSE 0 END ) AS IB_SESS
FROM {{ source('UA_REPORTS', 'RAW_EVENTS') }} 
GROUP BY 1
),

ALL_EVENTS AS (
SELECT 
    dimension4,
    IFNULL(CL_SB_FREE_CLOPS_TOT, 0) AS CL_SB_FREE_CLOPS_TOT,
    IFNULL(CL_SB_FREE_CLOPS_UNIQ, 0) AS CL_SB_FREE_CLOPS_UNIQ,
    IFNULL(CL_SB_FREE_CLOPS_SESS, 0) AS CL_SB_FREE_CLOPS_SESS,
    IFNULL(CL_SB_VAS_CLOPS_TOT, 0) AS CL_SB_VAS_CLOPS_TOT,
    IFNULL(CL_SB_VAS_CLOPS_UNIQ, 0) AS CL_SB_VAS_CLOPS_UNIQ,
    IFNULL(CL_SB_VAS_CLOPS_SESS, 0) AS CL_SB_VAS_CLOPS_SESS,
    IFNULL(CL_SB_ALL_CLOPS_SESS, 0) AS CL_SB_ALL_CLOPS_SESS,
    IFNULL(CL_NB_FREE_CLOPS_TOT, 0) AS CL_NB_FREE_CLOPS_TOT,
    IFNULL(CL_NB_FREE_CLOPS_UNIQ, 0) AS CL_NB_FREE_CLOPS_UNIQ,
    IFNULL(CL_NB_FREE_CLOPS_SESS, 0) AS CL_NB_FREE_CLOPS_SESS,
    IFNULL(CL_NB_PAID_CLOPS_TOT, 0) AS CL_NB_PAID_CLOPS_TOT,
    IFNULL(CL_NB_PAID_CLOPS_UNIQ, 0) AS CL_NB_PAID_CLOPS_UNIQ,
    IFNULL(CL_NB_PAID_CLOPS_SESS, 0) AS CL_NB_PAID_CLOPS_SESS,
    IFNULL(NB_NB_FREE_CLOPS_TOT, 0) AS NB_NB_FREE_CLOPS_TOT,
    IFNULL(NB_NB_FREE_CLOPS_UNIQ, 0) AS NB_NB_FREE_CLOPS_UNIQ,
    IFNULL(NB_NB_FREE_CLOPS_SESS, 0) AS NB_NB_FREE_CLOPS_SESS,
    IFNULL(NB_NB_PAID_CLOPS_TOT, 0) AS NB_NB_PAID_CLOPS_TOT,
    IFNULL(NB_NB_PAID_CLOPS_UNIQ, 0) AS NB_NB_PAID_CLOPS_UNIQ,
    IFNULL(NB_NB_PAID_CLOPS_SESS, 0) AS NB_NB_PAID_CLOPS_SESS,
    IFNULL(NB_NB_ALL_CLOPS_SESS, 0) AS NB_NB_ALL_CLOPS_SESS,
    IFNULL(ALL_REG_SUCCESS_TOT, 0) AS ALL_REG_SUCCESS_TOT,
    IFNULL(ALL_REG_SUCCESS_UNIQ, 0) AS ALL_REG_SUCCESS_UNIQ,
    IFNULL(ALL_REG_SUCCESS_SESS, 0) AS ALL_REG_SUCCESS_SESS,
    IFNULL(FL_REG_SUCCESS_TOT, 0) AS FL_REG_SUCCESS_TOT,
    IFNULL(FL_REG_SUCCESS_UNIQ, 0) AS FL_REG_SUCCESS_UNIQ,
    IFNULL(FL_REG_SUCCESS_SESS, 0) AS FL_REG_SUCCESS_SESS,
    IFNULL(P_AGENT_REG_SUCCESS_TOT, 0) AS P_AGENT_REG_SUCCESS_TOT,
    IFNULL(P_AGENT_REG_SUCCESS_UNIQ, 0) AS P_AGENT_REG_SUCCESS_UNIQ,
    IFNULL(P_AGENT_REG_SUCCESS_SESS, 0) AS P_AGENT_REG_SUCCESS_SESS,
    IFNULL(PARTNER_REG_SUCCESS_TOT, 0) AS PARTNER_REG_SUCCESS_TOT,
    IFNULL(PARTNER_REG_SUCCESS_UNIQ, 0) AS PARTNER_REG_SUCCESS_UNIQ,
    IFNULL(PARTNER_REG_SUCCESS_SESS, 0) AS PARTNER_REG_SUCCESS_SESS,
    IFNULL(IB_SESS, 0) AS IB_SESS,
FROM CL_CLOPS
FULL JOIN NB_CLOPS using (dimension4)
FULL JOIN REG_SUCCESS using (dimension4)
FULL JOIN OTHERS using (dimension4)
)
SELECT * FROM ALL_EVENTS
WHERE CL_SB_ALL_CLOPS_SESS + CL_NB_FREE_CLOPS_TOT + CL_NB_PAID_CLOPS_TOT + NB_NB_ALL_CLOPS_SESS + ALL_REG_SUCCESS_SESS + IB_SESS > 0