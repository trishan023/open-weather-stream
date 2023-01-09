import requests
import json
import pandas as pd
from datetime import datetime as dt
import os
from google.cloud import storage

# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator


# Function to keep time (because these things can take forever to run)
def time_taken(time: dt) -> str:
    s = (dt.now()-time).total_seconds()
    
    if s < 60:
        return f'{s} sec'
    else:
        m = int(s/60)
        s = int(s%60)
    
    if m < 60:
        return f'{m} min {s} sec'
    else:
        h = int(m/60)
        m = int(m%60)
        return f'{h} hr {m} min {s} sec'


# OpenWeather API config
api_key = '436bcf1b4529dcacc5c6e50841458fc2'
units = 'metric'

# Inout to the api call: a list of cities in the format -> "City, Country"
# locations = pd.read_csv('data/country_city.csv').head()
# cities = locations['location'].unique().tolist()

cities = ['Mumbai, India','London, UK']

# Google cloud storage config
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'credentials.json'
bucket_name = 'open-weather-api-data'


# Time Start
start_time = dt.now()


# Fetch data from the api
print('Sending request to the api..')
data = []
for city in cities:

    # Send the request
    response = requests.get(f'http://api.openweathermap.org/data/2.5/weather?q={city}&units={units}&appid={api_key}')
    # Responses can also be fetched using lat and lon. Refer to docs here: https://openweathermap.org/api/one-call-3

    # Parse the response
    data_json = json.loads(response.text)
    data.append(
            {
                'id' : data_json['weather'][0]['id'],
                'city' : city,
                'weather_short_desc' : data_json['weather'][0]['main'],
                'temperature' : data_json['main']['temp'],
                'min_temperature' : data_json['main']['temp_min'],
                'max_temperature' : data_json['main']['temp_max'],
                'pressure' : data_json['main']['pressure'],
                'humidity' : data_json['main']['humidity'],
                'wind_speed' : data_json['wind']['speed']
            }
        )

print('Responses collected from https://openweathermap.org/api/one-call-3')       


# Recording timestamp
current_time = dt.now().strftime("%Y-%m-%d %H:%M:%S")
filename = f'weather_data_{current_time}.csv'.replace(' ','_')


# Create a dataframe and save a cav copy
filename = f'weather_data_{current_time}.csv'.replace(' ','_')
df = pd.DataFrame(data)
df['time_of_record'] = current_time 
df.to_csv(filename,index=False)
print(f'Data collected: {df.shape} records')


# Pushing data to google cloud storage
storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)

blob = bucket.blob(filename)
blob.upload_from_filename(filename)
os.remove(filename) # removing the local version of the file

# Print the public URL of the file
print(f'Uploaded to google cloud storage bucket: "{bucket_name}" with filename as: "{filename}"')


# Time End
print(f'Total program runtime: {time_taken(start_time)}\n')           







'''
# Set up default arguments for the DAG
default_args = {
    'owner': 'me',
    'start_date': dt(2022, 1, 1),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'download_weather_data',
    default_args=default_args,
    schedule_interval='0 0 * * *',
    catchup=False
)

# Define the DAG task using the PythonOperator
task = PythonOperator(
    task_id='download_weather_data',
    python_callable=download_weather_data,
    dag=dag,
)

'''