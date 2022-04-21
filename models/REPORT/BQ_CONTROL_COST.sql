SELECT 
    EXTRACT(DATE FROM creation_time) AS date,
    user_email,
    job_type,
    statement_type,
    priority,
    SUM(total_bytes_billed) / POWER(1024, 3) AS total_bytes_billed_GB
FROM {{ source('INFORMATION_SCHEMA', 'JOBS_BY_PROJECT') }}
WHERE IFNULL(total_bytes_billed, 0) <> 0
GROUP BY 1,2,3,4,5
ORDER BY 1