SELECT
    trip_id,
    FORMAT_TIMESTAMP('%H:%M:00', TIMESTAMP_SECONDS(MIN(UNIX_SECONDS(timestamp)))) AS real_start_time,
    FORMAT_TIMESTAMP('%H:%M:00', TIMESTAMP_SECONDS(MAX(UNIX_SECONDS(timestamp)))) AS real_end_time,
FROM {{ source('raw_dataset', 'historical_stop_times') }}
GROUP BY trip_id
