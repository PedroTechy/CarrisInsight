SELECT
    id AS line_id,
    long_name AS line_name,
    routes AS list_routes,
    patterns AS list_patterns
FROM {{ source('raw_dataset', 'lines') }} l
QUALIFY ROW_NUMBER() OVER (PARTITION BY l.id ORDER BY l.ingested_at DESC) = 1