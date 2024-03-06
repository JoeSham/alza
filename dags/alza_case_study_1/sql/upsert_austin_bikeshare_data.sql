MERGE INTO alza_raw.austin_bikeshare AS dst
USING alza_tmp.tmp_austin_bikeshare AS tmp
ON tmp.date = dst.date AND tmp.station_id = dst.station_id AND tmp.bike_id = dst.bike_id
WHEN MATCHED THEN
    UPDATE SET
        station_name = tmp.station_name,
        minutes_in_use = tmp.minutes_in_use,
        updated_at = current_timestamp()
WHEN NOT MATCHED THEN
    INSERT (date, station_id, station_name, bike_id, minutes_in_use)
    VALUES(tmp.date, tmp.station_id, tmp.station_name, tmp.bike_id, tmp.minutes_in_use)
