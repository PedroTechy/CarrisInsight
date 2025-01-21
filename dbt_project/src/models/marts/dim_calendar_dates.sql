SELECT
    date,
    day_type,
    exception_type,
    holiday,
    period_name,
    service_id,
    CURRENT_TIMESTAMP() AS ingested_at
FROM {{ source('staging_dataset', 'calendar_dates') }}
