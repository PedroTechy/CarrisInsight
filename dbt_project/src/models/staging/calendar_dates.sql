SELECT
    PARSE_DATE('%Y%m%d', CAST(date AS STRING)) AS date, 
    CAST(day_type AS INT64) AS day_type,
    CAST(exception_type AS INT64) AS exception_type,
    CAST(holiday AS INT64) AS holiday,
    CAST(period AS INT64) AS period,
    pe.period_name AS period_name,
    CAST(service_id AS STRING) AS service_id
FROM {{ source('raw_dataset', 'calendar_dates') }} cd
LEFT JOIN {{ source('raw_dataset', 'periods') }} pe 
ON cd.period = pe.period_id