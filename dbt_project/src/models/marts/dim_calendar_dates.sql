SELECT
    FORMAT_DATE('%Y%m%d', date) as sk_date,
    date,
    day_type,
    exception_type,
    holiday,
    period,
    period_name,
    service_id,
FROM {{ source('staging_dataset', 'calendar_dates') }}
