SELECT
    r.id AS route_id,
    r.long_name AS route_name,
    r.line_id,
    STRING_AGG(m.name, ';') AS list_municipality_name,  -- concatenate municipality names with semicolons
    STRING_AGG(CAST(m.id AS STRING), ';') AS list_municipality_ids  -- concatenate municipality IDs with semicolons
FROM {{ source('raw_dataset', 'routes') }} r
LEFT JOIN {{ source('raw_dataset', 'municipalities') }} m
    ON m.id IN UNNEST(r.municipalities)
GROUP BY r.id, r.long_name, r.line_id
