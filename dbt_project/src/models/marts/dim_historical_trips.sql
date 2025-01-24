SELECT
  rst.trip_id,
  real_start_time,
  real_end_time,
  trip_date,
  CASE
    WHEN real_start_time > planned_start_time THEN TRUE
    ELSE FALSE
END
  AS departed_late,
  CASE
    WHEN real_end_time > planned_end_time THEN TRUE
    ELSE FALSE
END
  AS arrived_late,
  ( real_start_time - planned_start_time ) + ( real_end_time - planned_end_time ) AS accumulated_delay,
  CURRENT_TIMESTAMP() AS ingested_at
FROM
  {{ source('staging_dataset', 'real_stop_times') }}  rst
LEFT JOIN
  {{ source('marts_dataset', 'fact_trips') }} st
ON
  st.trip_id=rst.trip_id
  AND st.planned_date=rst.trip_date
