SELECT
    PARSE_DATE('%Y%m%d', CAST(date AS STRING)) AS date, 
    CAST(day_type AS INT64) AS day_type,
    CAST(exception_type AS INT64) AS exception_type,
    CAST(holiday AS INT64) AS holiday,
    CAST(period AS INT64) AS period,
    CAST(service_id AS STRING) AS service_id,
FROM {{ source('raw_dataset', 'calendar_dates') }}
