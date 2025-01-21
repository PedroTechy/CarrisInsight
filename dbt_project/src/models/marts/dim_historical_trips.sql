
-- add column that says if trip was delayed 

SELECT 
    trip_id,
    real_start_time,
    real_end_time,
    trip_date,
    CURRENT_TIMESTAMP() AS ingested_at
FROM {{ source('staging_dataset', 'real_stop_times') }}