(SELECT 
Date,
date_time,
CASE WHEN type in ('САЙТ','Квиз FB/Inst','Квиз VK')  
THEN 'Москва и Санкт-Петербург' else city END as city,
caller,
type,
SOLDNESS,
SOLD_SUM
FROM 
(SELECT
DATE(date_time) as Date,
date_time,
city,
caller,
CASE 
WHEN partner_source = 'Mumberry' then 'Mumberry'
WHEN partner_source = 'QUIZGO' then 'QUIZGO'
WHEN partner_source = 'Квиз FB/Inst' then 'Квиз FB/Inst'
WHEN partner_source = 'Квиз VK' or partner_source = 'VK лиды' then 'Квиз VK'
WHEN partner_source not in  ('Квиз FB/Inst','QUIZGO','Mumberry') and  source = 'Арбитраж' then 'Арбитраж'
ELSE  'САЙТ'
END as type,
partner_source,
IFNULL(sale_state, 'Не продан') as SOLDNESS,
CASE WHEN sale_state = 'Продан' then sold_sum else 0 END as SOLD_SUM
FROM {{ source('sheets', 'NB_ALL_CALLS') }} )
)