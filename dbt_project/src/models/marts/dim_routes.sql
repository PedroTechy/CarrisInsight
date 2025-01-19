{{
  config(
    materialized = 'table'
  )
}}

WITH route_municipalities AS (
    SELECT
        r.id as route_id,
        STRING_AGG(CONCAT(CAST(m.id AS STRING), '_', m.name), ';') AS municipalities_id_name
    FROM {{ source('raw_dataset', 'routes') }} r,
    UNNEST(r.municipalities) AS mun_id
    JOIN {{ source('raw_dataset', 'municipalities') }} m
        ON mun_id = m.id
    GROUP BY route_id
),

route_stops AS (
    SELECT
        stop_route_id as route_id,
        STRING_AGG(CONCAT(CAST(s.id AS STRING), '_', s.name), ';') AS stops_id_name
    FROM {{ source('raw_dataset', 'stops') }} s,
    UNNEST(s.routes) as stop_route_id
    GROUP BY stop_route_id
)

SELECT
    -- surrogate key route and line (can use others)
    {{ dbt_utils.generate_surrogate_key(['r.id', 'l.id']) }} as route_sk,
    r.id as route_id,
    r.long_name as route_long_name,
    l.id as line_id,
    l.long_name as line_long_name,
    rm.municipalities_id_name,
    rs.stops_id_name,
    current_timestamp() as ingested_at,
FROM {{ source('raw_dataset', 'routes') }} r
JOIN {{ source('raw_dataset', 'lines') }} l
    ON r.line_id = l.id
LEFT JOIN route_municipalities rm
    ON r.id = rm.route_id
LEFT JOIN route_stops rs
    ON r.id = rs.route_id