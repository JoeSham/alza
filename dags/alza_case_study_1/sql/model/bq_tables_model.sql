CREATE TABLE alza_raw.bikes(
  bike_id STRING,
  manufacturer STRING,
  price_eur BIGNUMERIC,  -- numeric can only handle 9 decimal places
  created_at TIMESTAMP DEFAULT current_timestamp(),
  updated_at TIMESTAMP DEFAULT current_timestamp(),
  PRIMARY KEY (bike_id) NOT ENFORCED,  -- BQ doesn't support enforced PKs yet
);

CREATE TABLE alza_raw.cnb_rates(
  date DATE,
  currency_code STRING,
  rate BIGNUMERIC,  -- numeric can only handle 9 decimal places
  created_at TIMESTAMP DEFAULT current_timestamp(),
  updated_at TIMESTAMP DEFAULT current_timestamp(),
  PRIMARY KEY (date, currency_code) NOT ENFORCED,  -- BQ doesn't support enforced PKs yet
);

CREATE TABLE alza_raw.cnb_currency_country_mapping(
  currency_code STRING,
  currency STRING,
  country STRING,
  created_at TIMESTAMP DEFAULT current_timestamp(),
  updated_at TIMESTAMP DEFAULT current_timestamp(),
  PRIMARY KEY (currency_code) NOT ENFORCED,
);

CREATE TABLE alza_raw.weather(
  date DATE,
  city STRING,
  temperature NUMERIC,
  created_at TIMESTAMP DEFAULT current_timestamp(),
  updated_at TIMESTAMP DEFAULT current_timestamp(),
  PRIMARY KEY (date, city) NOT ENFORCED,  -- BQ doesn't support enforced PKs yet
);

CREATE TABLE alza_raw.austin_bikeshare(
  date DATE,
  station_id INTEGER,
  station_name STRING,
  bike_id STRING,
  minutes_in_use INTEGER,
  created_at TIMESTAMP DEFAULT current_timestamp(),
  updated_at TIMESTAMP DEFAULT current_timestamp(),
  PRIMARY KEY (date, station_id, bike_id) NOT ENFORCED,  -- BQ doesn't support enforced PKs yet
);

CREATE TABLE alza.austin_bikeshare_enriched(
  date DATE,
  station_id INTEGER,
  station_name STRING,
  bike_id STRING,
  minutes_in_use INTEGER,
  temperature NUMERIC,
  manufacturer STRING,
  bike_price_czk NUMERIC,
  created_at TIMESTAMP DEFAULT current_timestamp(),
  updated_at TIMESTAMP DEFAULT current_timestamp(),
  PRIMARY KEY (date, station_id, bike_id) NOT ENFORCED,  -- BQ doesn't support enforced PKs yet
);
