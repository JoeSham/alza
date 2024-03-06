MERGE INTO alza_raw.cnb_rates AS dst
USING alza_tmp.tmp_cnb_rates AS tmp
ON tmp.date = dst.date AND tmp.currency_code = dst.currency_code
WHEN MATCHED THEN
    UPDATE SET
        rate = tmp.rate,
        updated_at = current_timestamp()
WHEN NOT MATCHED THEN
    INSERT (date, currency_code, rate) VALUES(tmp.date, tmp.currency_code, tmp.rate)
