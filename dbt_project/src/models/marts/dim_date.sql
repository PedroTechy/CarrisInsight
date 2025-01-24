SELECT
  DISTINCT dates.*,
  day_type,
  exception_type,
  holiday,
  period_name,
  CURRENT_TIMESTAMP() AS ingested_at
FROM
  {{ source('staging_dataset', 'dates') }}
LEFT JOIN
  {{ source('staging_dataset', 'calendar_dates') }}  cd
ON
  date_day=cd.date 