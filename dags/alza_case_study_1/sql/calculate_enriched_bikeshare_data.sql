DROP TABLE IF EXISTS alza_tmp.austin_bikeshare_enriched_prep;
CREATE TABLE alza_tmp.austin_bikeshare_enriched_prep AS
SELECT
  bs.date,
  bs.station_id,
  bs.station_name,
  bs.bike_id,
  bs.minutes_in_use,
  w.temperature,
  b.manufacturer,
  CAST(ROUND(b.price_eur * cnb.rate, 0) AS NUMERIC) AS bike_price_czk
FROM
  `alza-415512.alza_raw.austin_bikeshare` AS bs
  INNER JOIN `alza-415512.alza_raw.bikes` AS b ON bs.bike_id = b.bike_id
  INNER JOIN `alza-415512.alza_raw.cnb_rates` AS cnb ON bs.date = cnb.date AND cnb.currency_code = "EUR"
  INNER JOIN `alza-415512.alza_raw.weather` AS w ON bs.date = w.date AND w.city = "Austin, TX, United States"
ORDER BY 1, 2, 4;

MERGE INTO alza.austin_bikeshare_enriched AS dst
USING alza_tmp.austin_bikeshare_enriched_prep AS tmp
ON tmp.date = dst.date AND tmp.station_id = dst.station_id AND tmp.bike_id = dst.bike_id
WHEN MATCHED THEN
    UPDATE SET
        station_name = tmp.station_name,
        minutes_in_use = tmp.minutes_in_use,
        temperature = tmp.temperature,
        manufacturer = tmp.manufacturer,
        bike_price_czk = tmp.bike_price_czk,
        updated_at = current_timestamp()
WHEN NOT MATCHED THEN
    INSERT (date, station_id, station_name, bike_id, minutes_in_use, temperature, manufacturer, bike_price_czk)
    VALUES(tmp.date, tmp.station_id, tmp.station_name, tmp.bike_id, tmp.minutes_in_use, tmp.temperature,
           tmp.manufacturer, tmp.bike_price_czk);
