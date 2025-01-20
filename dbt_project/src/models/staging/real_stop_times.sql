SELECT
    trip_id,
    -- Get the start date (day) of the trip
    CAST(DATE(MIN(timestamp)) AS DATE) AS trip_date,  -- Extract the date from the earliest timestamp
    CAST({{ 
        handle_invalid_time( "FORMAT_TIMESTAMP('%H:%M:00', TIMESTAMP_SECONDS(MIN(UNIX_SECONDS(timestamp))))"  ) 
        }} AS TIME) AS real_start_time, 
    CAST({{ 
        handle_invalid_time("FORMAT_TIMESTAMP('%H:%M:00', TIMESTAMP_SECONDS(MAX(UNIX_SECONDS(timestamp))))"  ) 
        }} AS TIME) AS real_end_time
FROM {{ source('raw_dataset', 'historical_stop_times') }}
GROUP BY trip_id
