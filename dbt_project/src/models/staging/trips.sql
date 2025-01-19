SELECT
    trip_id,
    route_id,
    pattern_id,
    service_id,
    shape_id
FROM {{ source('raw_dataset', 'trips') }}