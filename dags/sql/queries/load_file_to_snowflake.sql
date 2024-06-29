COPY INTO city_database.new_city_schema.city_info
FROM @city_database.new_city_schema.snowflake_external_stage FILE_FORMAT = csv_format;