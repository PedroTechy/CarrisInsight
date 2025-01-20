SELECT 
    trip_id,
    real_start_time,
    real_end_time,
    trip_date
FROM {{ source('staging_dataset', 'real_stop_times') }}