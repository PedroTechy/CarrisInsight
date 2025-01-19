SELECT
    id AS line_id,
    long_name AS route_name,
    line_id,
    routes AS list_routes,
    patterns AS list_patterns
FROM {{ source('raw_dataset', 'lines') }}