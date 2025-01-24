WITH trip_dates AS (
    -- Get all possible dates for trips based on calendar and service_id
    SELECT DISTINCT
        t.trip_id,
        cd.date as service_date
    FROM {{ source('staging_dataset', 'trips') }} t
    JOIN {{ source('staging_dataset', 'calendar_dates') }} cd
        ON cd.service_id = t.service_id
)

SELECT DISTINCT
    st.trip_id,
    st.scheduled_start_time AS planned_start_time,
    st.scheduled_end_time AS planned_end_time,
    td.service_date as planned_date,
    st.shape_dist_traveled AS total_distance_traveled,
    st.number_of_stops,
    dr.sk_routes AS sk_routes,
    st.shape_dist_traveled * 0.001 / 
    (CASE 
        WHEN st.scheduled_end_time < st.scheduled_start_time 
        THEN (1440 - ABS(TIME_DIFF(st.scheduled_end_time, st.scheduled_start_time, MINUTE))) / 60.0 -- sub 1440 when trip ends after midnigth
        ELSE ABS(TIME_DIFF(st.scheduled_end_time, st.scheduled_start_time, MINUTE)) / 60.0
    END) AS average_speed_kmh,    
    t.direction_id as direction

FROM {{ source('staging_dataset', 'stop_times') }} st
LEFT JOIN {{ source('staging_dataset', 'trips') }} t
    ON t.trip_id = st.trip_id
LEFT JOIN {{ ref('dim_routes') }} dr
    ON t.route_id = dr.route_id
LEFT JOIN trip_dates td
    ON td.trip_id = st.trip_id

    --TODO: add total stops and total trip time Pedro