{{
  config(
    materialized = 'table',
    )
}}

with filtered as (
SELECT DISTINCT
departure_time,
arrival_time,
stop_sequence,
shape_dist_traveled,
trip_id
           
FROM {{ source('raw_dataset', 'stop_times') }}

QUALIFY
  ROW_NUMBER() OVER(PARTITION BY trip_id ORDER BY stop_sequence DESC) = 1
  OR ROW_NUMBER() OVER(PARTITION BY trip_id ORDER BY stop_sequence ASC) = 1
)


select 
LAG(departure_time, 1) OVER(PARTITION BY trip_id ORDER BY stop_sequence) AS start_time,
arrival_time,
stop_sequence,
shape_dist_traveled,
trip_id

from filtered
qualify
ROW_NUMBER() OVER(PARTITION BY trip_id ORDER BY stop_sequence DESC) = 1