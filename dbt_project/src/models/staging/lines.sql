SELECT
    id AS line_id,
    long_name AS line_name,
    routes AS list_routes,
    patterns AS list_patterns
FROM {{ source('raw_dataset', 'lines') }}