import requests
import json
import pandas as pd
import datetime
import os
from google.cloud import storage


# API arguments
api_key = '436bcf1b4529dcacc5c6e50841458fc2'
units = 'metric'
cities = ['London, UK','Mumbai, India','New York, US','Sydney, Australia']

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
                'weather_short_desc' : data_json['weather'][0]['main'],
                'city' : city,
                'temperature' : data_json['main']['temp'],
                'min_temperature' : data_json['main']['temp_min'],
                'max_temperature' : data_json['main']['temp_max'] ,
                'pressure' : data_json['main']['pressure'] ,
                'humidity' : data_json['main']['humidity'] ,
                'wind_speed' : data_json['wind']['speed']
            }
        )

# Recording timestamp
time_of_record = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Create the DataFrame and export as csv
csv_filename = f'weather_data_{time_of_record}.csv'
df = pd.DataFrame(data)
df['time_of_record'] = time_of_record 
df.to_csv(csv_filename,index=False)
print(df)


# Pushing data to google cloud storage
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'my-project-370707-aa60c05f2e05.json'
bucket_name = 'open-weather-api-data'

storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)

blob = bucket.blob(csv_filename)
blob.upload_from_filename(csv_filename)

# Print the public URL of the file
print(f"https://storage.googleapis.com/{bucket_name}/{csv_filename}")


