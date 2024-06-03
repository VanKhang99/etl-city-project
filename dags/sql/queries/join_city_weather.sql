SELECT 
    c.wiki_id AS wiki_id_city,
    name,
    country,
    population,
    latitude,
    longitude,
    weather_description,
    temperature_celsius,
    pressure,
    humidity,
    wind_speed,
    time_of_record,
    sunrise_time,
    sunset_time
FROM cities c
JOIN weathers w 
    ON c.wiki_id = w.fk_city;