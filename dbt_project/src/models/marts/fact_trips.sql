SELECT
    st.trip_id,
    rst.real_start_time,
    rst.real_end_time,
    st.scheduled_start_time,
    st.scheduled_end_time,
    st.shape_dist_traveled,
     (st.shape_dist_traveled * 0.001) / 
    (ABS(TIME_DIFF(st.scheduled_end_time, st.scheduled_start_time, MINUTE)) / 60.0) AS avg_velocity,
    t.pattern_id, -- Pattern ID from trips
    t.service_id
FROM {{ source('staging_dataset', 'stop_times') }} st
LEFT JOIN {{ source('staging_dataset', 'real_stop_times') }} rst 
    ON rst.trip_id = st.trip_id
-- Join with trips to get pattern_id, service_id, and shape_id
LEFT JOIN {{ source('staging_dataset', 'trips') }} t
    ON t.trip_id = st.trip_id
