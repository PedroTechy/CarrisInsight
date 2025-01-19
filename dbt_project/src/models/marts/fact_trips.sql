SELECT
    st.trip_id,
    rst.real_start_time,
    rst.real_end_time,
    st.scheduled_start_time,
    st.scheduled_end_time scheduled_start_time
FROM {{ source('staging_dataset', 'stop_times') }} st
LEFT JOIN {{ source('staging_dataset', 'real_stop_times') }} rst 
    ON rst.trip_id = st.trip_id



string distance_travelled
string direction
int service_id