SELECT
    r.id AS route_id,
    r.long_name AS route_name,
    r.line_id,
    ARRAY_AGG(m.name) AS list_municipality_name
FROM {{ source('raw_dataset', 'routes') }} r
LEFT JOIN {{ source('raw_dataset', 'municipalities') }} m
    ON m.id IN UNNEST(r.municipalities)
GROUP BY r.id, r.long_name, r.line_id
