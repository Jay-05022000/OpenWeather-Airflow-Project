
def Extraction():

    import requests
    import json
    import boto3
    import os  # To provide access of aws credentials stored as environment variables to the boto3 library.
    from botocore.exceptions import NoCredentialsError

    # AWS credentials are stored as os environment variables in the local system(Windows).

    API_key = 'a35ec550d80d3f07d953581a26f447a7'
    
    bucket = "openweather-etl-jay"

    # Initialize the Boto3 S3 client with default credentials
    s3_client = boto3.client('s3')

    def Current_Weather(city, Lat, Long):
        response = requests.get(f"https://api.openweathermap.org/data/3.0/onecall?lat={Lat}&lon={Long}&appid={API_key}&units=metric").json()
    
        Json_Data = json.dumps(response)
    
        key = f'Raw_Json_Data/Current_Weather/{city}_Current_Weather.json'
    
        try:
            s3_client.put_object(Bucket=bucket, Key=key, Body=Json_Data, ContentType='application/json')
            return f"{key} is successfully uploaded."
        
        except NoCredentialsError:
            return "Credentials not available for current weather."

    def Hourly_Weather_Forecast(city, Lat, Long):
    
        response = requests.get(f"https://api.openweathermap.org/data/3.0/onecall?lat={Lat}&lon={Long}&appid={API_key}&exclude=current,minutely,daily&units=metric").json()
    
        Json_Data = json.dumps(response)
    
        key = f'Raw_Json_Data/Hourly_Weather_Forecast/{city}_Hourly_Weather_Forecast.json'
    
        s3_client.put_object(Bucket=bucket, Key=key, Body=Json_Data, ContentType='application/json')
           
        return f"{key} is successfully uploaded."
    
    def Daily_Weather_Forecast(city, Lat, Long):
    
        response = requests.get(f"https://api.openweathermap.org/data/3.0/onecall?lat={Lat}&lon={Long}&appid={API_key}&exclude=current,minutely,hourly&units=metric").json()
    
        Json_Data = json.dumps(response)
    
        key = f'Raw_Json_Data/Daily_Weather_Forecast/{city}_Daily_Weather_Forecast.json'
    
        s3_client.put_object(Bucket=bucket, Key=key, Body=Json_Data, ContentType='application/json')
        
        return f"{key} is successfully uploaded."
    
       
    coordinates = {
    'Windsor': {'lat': '42.317432', 'long': '-83.026772'},
    'Ottawa': {'lat': '45.4247', 'long': '-75.695'},
    'Ahmedabad': {'lat': '23.0225', 'long': '72.5714'},
    'Perth': {'lat':'-31.887','long':'115.907'}
    }
    
    operation_status = []
    cities = []
    lat = []
    long = []

    for key,value in enumerate(coordinates):
        cities.append(value)
        lat.append(coordinates[value]['lat'])
        long.append(coordinates[value]['long'])
    
    for city in cities:
        current_weather = Current_Weather(city,lat[cities.index(city)],long[cities.index(city)])
        operation_status.append(current_weather)
        
        hourly_weather_forecast = Hourly_Weather_Forecast(city,lat[cities.index(city)],long[cities.index(city)])
        operation_status.append(hourly_weather_forecast)

        daily_weather_forecast = Daily_Weather_Forecast(city,lat[cities.index(city)],long[cities.index(city)])
        operation_status.append(daily_weather_forecast)

    return operation_status

status = Extraction()
print(status)