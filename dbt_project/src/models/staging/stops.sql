SELECT
    stop_id,
    name as stop_name,
    municipality_id,
    municipality_name,
    region_name,
    patterns as list_pattern,
    operational_status,
    routes
FROM {{ source('raw_dataset', 'stops') }}