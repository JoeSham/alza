-- has to be run in US location
SELECT
  CAST(DATE_TRUNC(tr.start_time, DAY) AS date) AS date,
  tr.start_station_id AS station_id,
  tr.start_station_name AS station_name,
  tr.bike_id,
  sum(tr.duration_minutes) AS minutes_in_use
FROM
  `bigquery-public-data.austin_bikeshare.bikeshare_trips` AS tr
  INNER JOIN `bigquery-public-data.austin_bikeshare.bikeshare_stations` AS st ON
    tr.start_station_id = st.station_id
WHERE
  st.status = "active"
  AND tr.start_station_id = CAST(tr.end_station_id AS int)
  AND tr.bike_type = "electric"
  AND tr.start_time >= '2021-01-01'
GROUP BY 1, 2, 3, 4
ORDER BY 1, 3, 4;
