CREATE TABLE IF NOT EXISTS weathers(
  fk_city varchar(20) REFERENCES cities(wiki_id) ON DELETE CASCADE,

  weather_description VARCHAR(255) NOT NULL,
  temperature_celsius DOUBLE PRECISION NOT NULL,
  pressure DOUBLE PRECISION,
  humidity DOUBLE PRECISION NOT NULL,
  wind_speed DOUBLE PRECISION NOT NULL,
  time_of_record timestamp NOT NULL,
  sunrise_time timestamp,
  sunset_time timestamp
);