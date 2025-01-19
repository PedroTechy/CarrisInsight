SELECT
    PARSE_DATE('%Y-%m-%d', dataPrevisao) AS date,
    CAST(tMax AS FLOAT64) AS max_temperature,
    CAST(tMin AS FLOAT64) AS min_temperature,
    CASE
        WHEN classIntVento = 1 THEN 'Weak'
        WHEN classIntVento = 2 THEN 'Moderate'
        WHEN classIntVento = 3 THEN 'Strong'
        WHEN classIntVento = 4 THEN 'Very strong'
        ELSE 'Unknown'
    END AS wind_speed_class,
    CASE
        WHEN classIntPrecipita = 0 THEN 'No precipitation'
        WHEN classIntPrecipita = 1 THEN 'Weak'
        WHEN classIntPrecipita = 2 THEN 'Moderate'
        WHEN classIntPrecipita = 3 THEN 'Strong'
        ELSE 'Unknown'
    END AS precipitation_class,
    CASE
        WHEN idTipoTempo = 1 THEN 'Clear sky'
        WHEN idTipoTempo = 2 THEN 'Partly cloudy'
        WHEN idTipoTempo = 3 THEN 'Sunny intervals'
        WHEN idTipoTempo = 4 THEN 'Cloudy'
        WHEN idTipoTempo = 5 THEN 'Cloudy (High cloud)'
        WHEN idTipoTempo = 6 THEN 'Showers/rain'
        WHEN idTipoTempo = 7 THEN 'Light showers/rain'
        WHEN idTipoTempo = 8 THEN 'Heavy showers/rain'
        WHEN idTipoTempo = 9 THEN 'Rain/showers'
        WHEN idTipoTempo = 10 THEN 'Light rain'
        WHEN idTipoTempo = 11 THEN 'Heavy rain/showers'
        WHEN idTipoTempo = 12 THEN 'Intermittent rain'
        WHEN idTipoTempo = 13 THEN 'Intermittent light rain'
        WHEN idTipoTempo = 14 THEN 'Intermittent heavy rain'
        WHEN idTipoTempo = 15 THEN 'Drizzle'
        WHEN idTipoTempo = 16 THEN 'Mist'
        WHEN idTipoTempo = 17 THEN 'Fog'
        WHEN idTipoTempo = 18 THEN 'Snow'
        WHEN idTipoTempo = 19 THEN 'Thunderstorms'
        WHEN idTipoTempo = 20 THEN 'Showers and thunderstorms'
        WHEN idTipoTempo = 21 THEN 'Hail'
        WHEN idTipoTempo = 22 THEN 'Frost'
        WHEN idTipoTempo = 23 THEN 'Rain and thunderstorms'
        WHEN idTipoTempo = 24 THEN 'Convective clouds'
        WHEN idTipoTempo = 25 THEN 'Partly cloudy'
        WHEN idTipoTempo = 26 THEN 'Fog'
        WHEN idTipoTempo = 27 THEN 'Cloudy'
        WHEN idTipoTempo = 28 THEN 'Snow showers'
        WHEN idTipoTempo = 29 THEN 'Rain and snow'
        WHEN idTipoTempo = 30 THEN 'Rain and snow'
        ELSE 'Unknown'
    END AS weather_type
FROM {{ source('raw_dataset', 'weather_data') }}