with filtered as (
    SELECT 
        departure_time,
        stop_sequence,
        shape_dist_traveled,
        trip_id
    FROM {{ source('raw_dataset', 'stop_times') }}
    QUALIFY 
        ROW_NUMBER() OVER (PARTITION BY trip_id ORDER BY stop_sequence DESC) = 1
        OR ROW_NUMBER() OVER (PARTITION BY trip_id ORDER BY stop_sequence ASC) = 1
),

total_stops as (
    SELECT 
        trip_id,
        COUNT(*) as number_of_stops
    FROM {{ source('raw_dataset', 'stop_times') }}
    GROUP BY trip_id
),

filtered_with_times as (
    SELECT 
        trip_id,
        stop_sequence,
        shape_dist_traveled,
        CAST({{ handle_invalid_time('LAG(departure_time, 1) OVER(PARTITION BY trip_id ORDER BY stop_sequence)') }} AS TIME) AS scheduled_start_time,
        CAST({{ handle_invalid_time('departure_time') }} AS TIME) AS scheduled_end_time
    FROM filtered
),

final_selection as (
    SELECT 
        f.scheduled_start_time,
        f.scheduled_end_time,
        f.stop_sequence,
        f.shape_dist_traveled,
        f.trip_id,
        ts.number_of_stops
    FROM filtered_with_times f
    JOIN total_stops ts
    ON f.trip_id = ts.trip_id
)

SELECT *
FROM final_selection
QUALIFY ROW_NUMBER() OVER (PARTITION BY trip_id ORDER BY stop_sequence DESC) = 1
