DROP TABLE IF EXISTS city_info;

CREATE TABLE IF NOT EXISTS city_info(
    wiki_id_city VARCHAR(20) NOT NULL,
    name VARCHAR(80) NOT NULL,
    country VARCHAR(100) NOT NULL,
    population BIGINT NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    weather_description VARCHAR(255) NOT NULL,
    temperature_celsius DOUBLE PRECISION NOT NULL,
    pressure DOUBLE PRECISION,
    humidity DOUBLE PRECISION NOT NULL,
    wind_speed DOUBLE PRECISION NOT NULL,
    time_of_record timestamp NOT NULL,
    sunrise_time timestamp,
    sunset_time timestamp
)