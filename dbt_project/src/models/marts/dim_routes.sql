WITH route_stops AS (
    SELECT
        stop_route_id AS route_id,
        STRING_AGG(CAST(s.stop_id AS STRING), ';') AS stops_ids,  -- String of stop IDs
        STRING_AGG(s.stop_name, ';') AS stops_names  -- String of stop names
    FROM {{ source('staging_dataset', 'stops') }} s,
    UNNEST(s.routes) AS stop_route_id
    GROUP BY stop_route_id
)

SELECT
    -- Surrogate key route and line (generated by dbt_utils.generate_surrogate_key)
    {{ dbt_utils.generate_surrogate_key(['r.route_id', 'l.line_id']) }} AS sk_routes,
    r.route_id AS route_id,
    l.line_id AS line_id,
    r.route_name,
    l.line_name,
    r.list_municipality_name,  
    r.list_municipality_ids, 
    rs.stops_ids,  
    rs.stops_names, 
    CURRENT_TIMESTAMP() AS ingested_at
FROM {{ source('staging_dataset', 'routes') }} r
JOIN {{ source('staging_dataset', 'lines') }} l
    ON r.line_id = l.line_id
LEFT JOIN route_stops rs
    ON r.route_id = rs.route_id