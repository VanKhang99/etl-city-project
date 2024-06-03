from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import json
import os
import pandas as pd

#######################################################################################
################################### COMMON FUNCTION ###################################
#######################################################################################
def to_csv(df_data, name_file):
  time_now = datetime.now() + timedelta(hours=7)
  
  file_name = f'{name_file}_data_{time_now.strftime('%d-%m-%Y')}'
  df_data.to_csv(f'/opt/airflow/data-transformed/{file_name}.csv', index=False)

  return f'{file_name}.csv'

def load_csv_to_postgres(**kwargs):
  ti = kwargs['ti']
  is_weather = kwargs['is_weather']
  task_ids = kwargs['task_ids']
  table_name=kwargs['table_name']
  file_name = ti.xcom_pull(task_ids=f'group_tasks.{task_ids}' if is_weather else f'{task_ids}')['file_name']

  hook = PostgresHook(postgres_conn_id='postgres_connection_id')
  conn = hook.get_conn()
  cursor = conn.cursor()
    
  with open(f'/opt/airflow/data-transformed/{file_name}', 'r') as f:
      cursor.copy_expert(f'COPY {table_name} FROM stdin WITH CSV HEADER', f)
  
  conn.commit()
  cursor.close()
  conn.close()

def save_data_joined_to_csv(**kwargs):
  ti = kwargs['ti']
  data_joined = ti.xcom_pull(task_ids='join_city_weather', key = 'return_value')
  wiki_id = ti.xcom_pull(task_ids='transform_data_city_api')['wiki_id']

  df_data = pd.DataFrame(data_joined, columns=[
    'wiki_id_city',
    'name',
    'country',
    'population',
    'latitude',
    'longitude',
    'weather_description',
    'temperature_celsius',
    'pressure',
    'humidity',
    'wind_speed',
    'sunrise_time',
    'sunset_time'
  ])
  file_name = to_csv(df_data, f'{wiki_id}_joined')
  return file_name

def up_load_data_joined_to_s3(**kwargs):
  ti = kwargs['ti']
  file_name = ti.xcom_pull(task_ids ='save_data_joined_to_csv')

  hook = S3Hook('weather_map_s3_connection')
  hook.load_file(filename=f'/opt/airflow/data-transformed/{file_name}', key=file_name, bucket_name='data-about-city')
  
def delete_files_csv():
  files = os.listdir('/opt/airflow/data-transformed')
  try:
    for file in files:
      file_path = f'/opt/airflow/data-transformed/{file}'
      if os.path.isfile(file_path):
        os.remove(file_path)
      print('All files deleted successfully')
  except OSError:
     print("Error occurred while deleting files.")

############################################################################
################################### CITY ###################################
############################################################################
def kelvin_to_celsius(temp_in_kelvin):
  temp_in_celsius = temp_in_kelvin - 273.15
  return round(temp_in_celsius, 2)

def transform_data_city_api(**kwargs):
  ti = kwargs['ti']
  res = ti.xcom_pull(task_ids='extract_city_data_from_api')
  data = res['data']

  wiki_id = data['wikiDataId']
  country = data['country']
  name = data['name']
  population = data['population']
  latitude = data['latitude']
  longitude = data['longitude']

  transformed_data = {
    'wiki_id': wiki_id,
    'name': name,
    'country': country,
    'population': population,
    'latitude': latitude,
    'longitude': longitude
  }

  df_data = pd.DataFrame([transformed_data])
  file_name = to_csv(df_data, f'{wiki_id}_city')
  return {
    'wiki_id': wiki_id,
    'file_name': file_name,
    'wiki_id': wiki_id,
    'latitude': latitude,
    'longitude': longitude
  }

def check_data_city_is_exist(**kwargs):
  ti = kwargs['ti']
  wiki_id = ti.xcom_pull(task_ids=f'transform_data_city_api')['wiki_id']

  query = f"SELECT COUNT(*) FROM cities WHERE wiki_id = '{wiki_id}';"
  hook = PostgresHook(postgres_conn_id='postgres_connection_id')
  records = hook.get_records(query)
  
  if records[0][0] >= 1:
    return f'end_tasks_flow'
  else:
    return f'group_tasks.load_csv_city_to_postgres'

###############################################################################
################################### WEATHER ###################################
###############################################################################
def is_weather_api_ready(**kwargs):
  sensor = HttpSensor(
    task_id = 'is_api_ready',
    http_conn_id = kwargs['http_conn_id'],
    endpoint = f'/data/2.5/weather?lat={kwargs['latitude']}&lon={kwargs['longitude']}&appid={kwargs['API_KEY']}'
  )

  sensor.poke(context=kwargs)

def extract_weather_data_from_api(**kwargs):
  ti = kwargs['ti']
  operator = SimpleHttpOperator(
    task_id = 'extract_data',
    http_conn_id = kwargs['http_conn_id'],
    endpoint = f'/data/2.5/weather?lat={kwargs['latitude']}&lon={kwargs['longitude']}&appid={kwargs['API_KEY']}',
    method = 'GET',
    response_filter = lambda response: json.loads(response.text),
    log_response = True
  )

  response = operator.execute(context=kwargs)
  ti.xcom_push('data_weanter', response)

def transform_data_weather_api(**kwargs):
  ti = kwargs['ti']
  wiki_id = ti.xcom_pull(task_ids='transform_data_city_api')['wiki_id']
  data = ti.xcom_pull(task_ids='group_tasks.extract_weather_data_from_api', key='data_weanter')

  weather_description = data['weather'][0]['description']
  temperature_celsius = kelvin_to_celsius(data['main']['temp'])
  pressure = data['main']['pressure']
  humidity = data['main']['humidity']
  wind_speed = data['wind']['speed']
  time_of_record = datetime.fromtimestamp(data['dt']) + timedelta(hours=7)
  sunrise_time = datetime.fromtimestamp(data['sys']['sunrise']) + timedelta(hours=7)
  sunset_time = datetime.fromtimestamp(data['sys']['sunset']) + timedelta(hours=7)

  transformed_data = {
    'fk_city': wiki_id,
    'weather_description': weather_description,
    'temperature_celsius': temperature_celsius,
    'pressure': pressure,
    'humidity': humidity,
    'wind_speed': wind_speed,
    'time_of_record': time_of_record,
    'sunrise_time': sunrise_time,
    'sunset_time': sunset_time
  }

  df_data = pd.DataFrame([transformed_data])
  file_name_weather = to_csv(df_data, f'{wiki_id}_weather')
  return {
    'fk_city': wiki_id,
    'file_name': file_name_weather
  }
##################################################################################
################################### TASKS FLOW ###################################
##################################################################################
default_args = {
  'owner': 'Khang',
  'retries': 5,
  'retry_delay': timedelta(minutes=5)
}

with DAG (
  default_args = default_args,
  dag_id = 'city_data_workflow_DAG_final',
  description = 'Call city and weather API, insert to PosgreSQL and upload that data to AWS S3',
  start_date= datetime(2024, 6, 3),
  schedule_interval = '@daily',
  catchup= False
) as dag:

  is_city_api_ready = HttpSensor(
    task_id = 'is_api_city_ready',
    http_conn_id = 'city_map_api_connection',
    endpoint = '/v1/geo/cities/Q25282',
    headers = {
        "X-RapidAPI-Key": "94e1cf3537mshb395c2b8765d914p1dff7fjsnba7ed4fb8d55"
    }
  )

  extract_city_data_from_api = SimpleHttpOperator(
    task_id = 'extract_city_data_from_api',
    http_conn_id = 'city_map_api_connection',
    endpoint ='/v1/geo/cities/Q25282',
    headers = {
        "X-RapidAPI-Key": "94e1cf3537mshb395c2b8765d914p1dff7fjsnba7ed4fb8d55"
    },
    method = 'GET',
    response_filter = lambda response: json.loads(response.text),
    log_response = True
  )

  transform_data_city_api = PythonOperator(
    task_id = 'transform_data_city_api',
    python_callable = transform_data_city_api
  )

  join_city_weather = PostgresOperator(
    task_id = 'join_city_weather',
    postgres_conn_id  = 'postgres_connection_id',
    sql = 'sql/queries/join_city_weather.sql'
  )

  save_data_joined_to_csv = PythonOperator(
    task_id ='save_data_joined_to_csv',
    python_callable = save_data_joined_to_csv
  )

  up_load_data_joined_to_s3 = PythonOperator(
    task_id = 'up_load_data_joined_to_s3',
    python_callable = up_load_data_joined_to_s3,
  )

  delete_files_csv = PythonOperator(
    task_id = 'delete_files_csv',
    python_callable = delete_files_csv
  )

  end_tasks_flow = DummyOperator(
    task_id ='end_tasks_flow'
  )

  with TaskGroup(group_id = 'group_tasks') as group_tasks:

    create_table_city_postgresql = PostgresOperator(
      task_id = 'create_table_city_postgresql',
      postgres_conn_id  = 'postgres_connection_id',
      sql = 'sql/create/create_table_cities.sql'
    )

    check_data_city_is_exist = BranchPythonOperator(
      task_id = 'check_data_city_is_exist',
      python_callable = check_data_city_is_exist
    )

    load_csv_city_to_postgres = PythonOperator(
      task_id = 'load_csv_city_to_postgres',
      python_callable=load_csv_to_postgres,
      op_kwargs = {
        'task_ids': 'transform_data_city_api',
        'table_name': 'cities',
        'is_weather': False
      }
    )    

    ################################### WEATHER ###################################
    is_weather_api_ready = PythonOperator(
      task_id = 'is_weather_api_ready',
      python_callable = is_weather_api_ready,
      op_kwargs = {
        'http_conn_id': 'weather_map_api_connection',
        'latitude': '{{ ti.xcom_pull(task_ids="transform_data_city_api")["latitude"] }}',
        'longitude': '{{ ti.xcom_pull(task_ids="transform_data_city_api")["longitude"] }}',
        'API_KEY': 'cb9774016f254a4cb1aa6516bf401afa'
      },
      provide_context=True
    ) 

    extract_weather_data_from_api = PythonOperator(
      task_id = 'extract_weather_data_from_api',
      python_callable = extract_weather_data_from_api,
      op_kwargs = {
        'http_conn_id': 'weather_map_api_connection',
        'latitude': '{{ ti.xcom_pull(task_ids="transform_data_city_api")["latitude"] }}',
        'longitude': '{{ ti.xcom_pull(task_ids="transform_data_city_api")["longitude"] }}',
        'API_KEY': 'cb9774016f254a4cb1aa6516bf401afa'
      },
      provide_context=True
    )

    transform_data_weather_api = PythonOperator(
      task_id = 'transform_data_weather_api',
      python_callable = transform_data_weather_api
    )

    create_table_weather_postgresql = PostgresOperator(
      task_id = 'create_table_weather_postgresql',
      postgres_conn_id  = 'postgres_connection_id',
      sql = 'sql/create/create_table_weathers.sql'
    )

    load_csv_weather_to_postgres = PythonOperator(
      task_id = 'load_csv_weather_to_postgres',
      python_callable=load_csv_to_postgres,
      op_kwargs = {
        'task_ids': 'transform_data_weather_api',
        'table_name': 'weathers',
        'is_weather': True
      }
    )

    ################################### TASKS FLOW ###################################
    create_table_city_postgresql >> check_data_city_is_exist >> [load_csv_city_to_postgres, end_tasks_flow]
    
    load_csv_city_to_postgres >> create_table_weather_postgresql
   
    is_weather_api_ready >> extract_weather_data_from_api >> transform_data_weather_api >> create_table_weather_postgresql >> load_csv_weather_to_postgres

   

  is_city_api_ready >> extract_city_data_from_api >> transform_data_city_api >> group_tasks >> join_city_weather >> save_data_joined_to_csv >> up_load_data_joined_to_s3 >> delete_files_csv
  
  

  

  


 





