SELECT
    date,
    day_type,
    exception_type,
    holiday,
    period,
    period_name,
    service_id,
FROM {{ source('staging_dataset', 'calendar_dates') }}
