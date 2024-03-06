MERGE INTO alza_raw.bikes AS dst
USING alza_tmp.tmp_bikes AS tmp
ON tmp.bike_id = dst.bike_id
WHEN MATCHED THEN
    UPDATE SET
        manufacturer = tmp.manufacturer,
        price_eur = tmp.price_eur,
        updated_at = current_timestamp()
WHEN NOT MATCHED THEN
    INSERT (bike_id, manufacturer, price_eur) VALUES(tmp.bike_id, tmp.manufacturer, tmp.price_eur)
