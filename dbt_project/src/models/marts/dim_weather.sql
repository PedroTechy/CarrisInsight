SELECT
    date, 
    max_temperature,
    min_temperature,
    wind_speed_class,
    precipitation_class,
    weather_type,
    CURRENT_TIMESTAMP() AS ingested_at
FROM {{ source('staging_dataset', 'weather_data') }} 
