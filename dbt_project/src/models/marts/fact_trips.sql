SELECT
    st.trip_id,  -- Direct from staging, no surrogate key for trips
    rst.real_start_time AS actual_start_time,
    rst.real_end_time AS actual_end_time,
    st.scheduled_start_time AS planned_start_time,
    st.scheduled_end_time AS planned_end_time,
    st.shape_dist_traveled AS total_distance_traveled,
    dr.sk_routes AS route_key  -- Surrogate key from dim_routes for routes
    (st.shape_dist_traveled * 0.001) / 
    (ABS(TIME_DIFF(st.scheduled_end_time, st.scheduled_start_time, MINUTE)) / 60.0) AS average_speed_kmh,
    t.pattern_id AS route_pattern_id,
    t.service_id,
FROM {{ source('staging_dataset', 'stop_times') }} st
LEFT JOIN {{ source('staging_dataset', 'real_stop_times') }} rst 
    ON rst.trip_id = st.trip_id
LEFT JOIN {{ source('staging_dataset', 'trips') }} t
    ON t.trip_id = st.trip_id
LEFT JOIN {{ ref('dim_routes') }} dr  -- Link to dim_routes using route_id
    ON t.route_id = dr.route_id
