MERGE INTO alza_raw.weather AS dst
USING alza_tmp.tmp_weather AS tmp
ON tmp.city = dst.city AND tmp.date = dst.date
WHEN MATCHED THEN
    UPDATE SET
        city = tmp.city,
        date = tmp.date,
        temperature = tmp.temperature,
        updated_at = current_timestamp()
WHEN NOT MATCHED THEN
    INSERT (city, date, temperature) VALUES(tmp.city, tmp.date, tmp.temperature)
