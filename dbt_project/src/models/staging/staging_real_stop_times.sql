{{
  config(
    materialized = 'table',
    )
}}

WITH real_times AS (
    SELECT
        trip_id,
        FORMAT_TIMESTAMP('%H:%M:00', TIMESTAMP_SECONDS(MIN(UNIX_SECONDS(timestamp)))) AS real_start_time,
        FORMAT_TIMESTAMP('%H:%M:00', TIMESTAMP_SECONDS(MAX(UNIX_SECONDS(timestamp)))) AS real_end_time,
        COUNT(stop_id) AS total_stops
    FROM {{ source('raw_dataset', 'historical_stop_times') }} real_times_table
    GROUP BY trip_id
),
first_last_stop AS (
    SELECT
        trip_id,
        -- this array agr was gpt suggestion, as I am not the most versated person on sql there m8 be a smarter/efficient/better way to do it, ask team
        -- stop_id of the first stop (based on earliest timestamp)
        ARRAY_AGG(stop_id ORDER BY timestamp ASC LIMIT 1)[OFFSET(0)] AS first_stop_id,
        -- stop_id of the last stop (based on latest timestamp)
        ARRAY_AGG(stop_id ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)] AS last_stop_id
    FROM {{ source('raw_dataset',  'historical_stop_times') }}
    GROUP BY trip_id
),
tabulated_times AS (
    SELECT
        trip_id,
        start_time AS tabulated_start_time,
        arrival_time AS tabulated_end_time
    FROM {{ ref('staging_stop_times') }} 
)
SELECT
    rt.trip_id,
    rt.real_start_time,
    rt.real_end_time,
    fl.first_stop_id,
    fl.last_stop_id,
    rt.total_stops,
    tab.tabulated_start_time,
    tab.tabulated_end_time
FROM real_times rt
JOIN first_last_stop fl
    ON rt.trip_id = fl.trip_id
JOIN tabulated_times tab
    ON rt.trip_id = tab.trip_id