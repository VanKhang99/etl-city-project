# ETL CITY DATA PROJECT
This project will call the city api (via wikiCityId) to get latitude and longitude, then continue to call the weather api via latitude and longitude to get data about the city's weather.

Then transform the data and insert it into posgreSQL. Finally, upload to AWS S3.

## To run this project:
### Step 1: Run command 
docker compose up airflow-init
### Step 2: Run command 
docker compose up -d
