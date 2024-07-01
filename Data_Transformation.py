
def Transformation():

    import pandas as pd
    import boto3
    import os
    import datetime
    import pytz
    import json
    from io import StringIO

    bucket = 'openweather-etl-jay'
   
    
    s3 = boto3.client('s3')
    
    def Time_conversion(unix_time,time_zone): 
 
        # Convert Unix time to a datetime object in UTC
        utc_time = datetime.datetime.utcfromtimestamp(unix_time).replace(tzinfo=pytz.utc)

        # Define the target time zone
        desired_time_zone = pytz.timezone(time_zone)

        # Convert UTC time to Toronto time
        time_in_desired_zone = utc_time.astimezone(desired_time_zone)

        # Format the Toronto time into a human-readable string
        formatted_time = time_in_desired_zone.strftime('%Y:%m:%d %H:%M:%S')
    
        return  formatted_time
    
    def Day_Conversion(date_time):
        
        # Create dictionary to get month name.
        months = {'01' : 'Jan','02' : 'Feb','03' : 'March','04' : 'Apr','05' : 'May','06' : 'June','07' : 'Jul','08' : 'Aug','09' : 'Sep','10' : 'Oct','11' : 'Nov','12' : 'Dec'}
        
        # Fetch date information from date_time.
        date = date_time.split(' ')[0]
        
        # Get month information in words using date_time and months dictionary.
        month = months[date.split(':')[1]] # Get month information from the date.

        # create a exponent for date based on the day number.

        if int((date.split(':')[2])[-1]) == 1:
            mid = "st"
        elif int((date.split(':')[2])[-1]) == 2:
            mid = "nd"
        elif int((date.split(':')[2])[-1]) == 3:
            mid = "rd"
        else:
            mid = "th"

        # Standardize the format of day.
        try:
            if int(date.split(':')[2]) < 10:
                day = (date.split(':')[2])[1] + mid + ' ' + month
                return day
            else:
                day = date.split(':')[2] + mid + ' ' + month
                return day
        except Exception as e:
            return print(f"An error occured : {e} for {date_time}")
    
    
    def Transform_Current_Weather():

        json_files = s3.list_objects(Bucket = bucket , Prefix = 'Raw_Json_Data/Current_Weather/')

        current_Weather =[]

        for file in json_files['Contents']:
            file_name = file['Key']  # file path.
    
            json_file = s3.get_object(Bucket = bucket,Key = file_name)  # Get the json file.
            content = json_file['Body'].read()  # To read the content of StreamingBody object.
            json_data = json.loads(content)  # To transform content into proper json structure.
    

            city = ((file_name.split('/')[2]).split('.')[0]).split('_')[0]  # create city column from file_name.
            time_zone = json_data['timezone']
            time_zone_offset = json_data['timezone_offset']
            current_time = Time_conversion(json_data['current']['dt'],time_zone).split(' ')[1]  # current time in local time-zone.
            sunrise = Time_conversion(json_data['current']['sunrise'],time_zone).split(' ')[1]
            sunset = Time_conversion(json_data['current']['sunset'],time_zone).split(' ')[1]
            temp = json_data['current']['temp']
            feels_like = json_data['current']['feels_like']
            pressure =  json_data['current']['pressure']
            humidity = json_data['current']['humidity']
            uvi = json_data['current']['uvi']
            clouds = json_data['current']['clouds']
            visibility = json_data['current']['visibility']
            wind_speed = json_data['current']['wind_speed']
            wind_deg = json_data['current']['wind_deg']
            overall = json_data['current']['weather'][0]['main']
            description = json_data['current']['weather'][0]['description']

            weather = {'City' : city,
            'Time Zone' : time_zone,
            'Offset' : time_zone_offset,
            'Current Time' : current_time,
            'Sunrise' : sunrise,
            'Sunset' : sunset,
            'Temparature':temp,
            'Feels Like' : feels_like,
            'Pressure' : pressure,
            'Humidity' : uvi,
            'Clouds' : clouds,
            'Visibility' : visibility,
            'Wind Speed' : wind_speed,
            'Degree' : wind_deg,
            'Overall Weather' : overall,
            'Weather Description' : description}

            current_Weather.append(weather)


        Current_Weather = pd.DataFrame.from_dict(current_Weather)

        # Upload pandas dataframe into s3 bucket in csv formate.
    
        Csv_buffer = StringIO()  # create a buffer object.
        Current_Weather.to_csv(Csv_buffer)  # convert pandas dataframe to csv and temporarily store in buffer object before uploading it.
    
        destination = 'Transformed_CSV_Data/Current_Weather/Current_Weather.csv'
    
        # Upload csv file into desired s3 folder.
    
        s3.put_object(Bucket = bucket,Key = destination,Body = Csv_buffer.getvalue(),ContentType='text/csv')
    
        return "Current_Weather file has been successfully uploaded.\n"
    
    
    def Transform_Hourly_Weather_Forecast():
    
        json_files = s3.list_objects(Bucket = bucket , Prefix = 'Raw_Json_Data/Hourly_Weather_Forecast/')

        hourly_weather_forecast =[]

        for file in json_files['Contents']:
            file_name = file['Key']  # file path.

            json_file = s3.get_object(Bucket = bucket,Key = file_name)  # Get the json file.
            content = json_file['Body'].read()  # To read the content of StreamingBody object.
            json_data = json.loads(content)  # To transform content into proper json structure.


            city = ((file_name.split('/')[2]).split('.')[0]).split('_')[0]  # create city column from file_name.
            time_zone = json_data['timezone']

            for hour in json_data['hourly']:   # Access each hour's forecast from API response.
                index = json_data['hourly'].index(hour)  # Create an index of that hour.

                if index < 12:   # only fetch forecast data for the upcoming 24 hours(API gives 48 hours of forecast)

                    time = Time_conversion(hour['dt'],time_zone).split(' ')[1]
                    hour_info = time.split(':')[0] + ':' + time.split(':')[1]  # To trim time into HH:MM formate.
                    temp = hour['temp']
                    feels_like = hour['feels_like']
                    humidity = hour['humidity'] # Amount of water vapor in the air.
                    clouds = hour['clouds']
                    visibility = hour['visibility']
                    wind_speed = hour['wind_speed']
                    pop = hour['pop']  # Chances of rain.
                    overall = hour['weather'][0]['main']
                    description = hour['weather'][0]['description']

                    # create a dictionary to store individaual hour information.

                    hourly_weather = {'City' : city,
                    'Time' : time,
                    'Hour' : hour_info,
                    'Temperature' : temp,
                    'Feels Like' : feels_like,
                    'Humidity' : humidity,
                    'Clouds' : clouds,
                    'Visibility' : visibility,
                    'Wind Speed' : wind_speed,
                    'PoP' : pop,
                    'Overall' : overall,
                    'Description' : description
                    }

                    hourly_weather_forecast.append(hourly_weather) # Append dictionary to the list.
        
                else:
                    None
    
        # Create a pandas dataframe from 'hourly_weather_forecast' list.

        Hourly_Weather_Forecast = pd.DataFrame.from_dict(hourly_weather_forecast)

        # Upload pandas dataframe into s3 bucket in csv formate.

        Csv_buffer = StringIO()  # create a buffer object.
        Hourly_Weather_Forecast.to_csv(Csv_buffer)  # convert pandas dataframe to csv and temporarily store in buffer object before uploading it.

        destination = 'Transformed_CSV_Data/Hourly_Weather_Forecast/Hourly_Weather_Forecast.csv'

        # Upload csv file into desired s3 folder.

        s3.put_object(Bucket = bucket,Key = destination,Body = Csv_buffer.getvalue(),ContentType='text/csv')

        return "Hourly_Weather_Forecast file has been successfully uploaded.\n"

    
    
    def Transform_Daily_Weather_Forecast():
    
        json_files = s3.list_objects(Bucket = bucket , Prefix = 'Raw_Json_Data/Daily_Weather_Forecast/')

        Daily_weather_forecast =[]

        for file in json_files['Contents']:
            file_name = file['Key']  # file path.

            json_file = s3.get_object(Bucket = bucket,Key = file_name)  # Get the json file.
            content = json_file['Body'].read()  # To read the content of StreamingBody object.
            json_data = json.loads(content)  # To transform content into proper json structure.


            city = ((file_name.split('/')[2]).split('.')[0]).split('_')[0]  # create city column from file_name.
            time_zone = json_data['timezone']

            for day in json_data['daily']:   # Daily weather forecast data.

                date_time = (Time_conversion(day['dt'],time_zone).split(' '))[0]
                day_info = Day_Conversion(date_time)
                sunrise = (Time_conversion(day['sunrise'],time_zone).split(' '))[1] 
                sunset = (Time_conversion(day['sunset'],time_zone).split(' '))[1] 
                moonrise = (Time_conversion(day['moonrise'],time_zone).split(' '))[1] 
                moonset = (Time_conversion(day['moonset'],time_zone).split(' '))[1] 
                summary = day['summary'].replace(',', '')  # Remove comma.Some summary values contains comma in them and that might cause co;umn values interpretion error by glue crawler while creating athena tables.
                max_temp = day['temp']['max']
                min_temp = day['temp']['min'] 
                day_temp = day['temp']['day'] 
                day_feels_like = day['feels_like']['day'] 
                humidity = day['humidity']
                wind_speed = day['wind_speed']
                overall = day['weather'][0]['main']
                description = day['weather'][0]['description']
                pop = day['pop'] 
                uvi = day['uvi']

                data = {'City' : city,
                'Time_zone' : time_zone,
                'Date_time' : date_time,
                'Day' : day_info,
                'Sunrise' : sunrise,
                'Sunset' : sunset,
                'Moonrise' : moonrise,
                'Moonset' : moonset,
                'Summary' : summary,
                'Max_temp' : max_temp,
                'Min_temp' : min_temp,
                'Day_temp' : day_temp,
                'Day_feels_like' : day_feels_like,
                'Humidity' : humidity,
                'Wind speed' : wind_speed,
                'Overall' : overall,
                'Description' : description,
                'PoP' : pop,  
                'Uvi' : uvi,
                }

                Daily_weather_forecast.append(data)   



        # Create a pandas dataframe from 'hourly_weather_forecast' list.

        Daily_Weather_Forecast = pd.DataFrame.from_dict(Daily_weather_forecast)

        # Upload pandas dataframe into s3 bucket in csv formate.

        Csv_buffer = StringIO()  # create a buffer object.
        Daily_Weather_Forecast.to_csv(Csv_buffer)  # convert pandas dataframe to csv and temporarily store in buffer object before uploading it.

        destination = 'Transformed_CSV_Data/Daily_Weather_Forecast/Daily_Weather_Forecast.csv'

        # Upload csv file into desired s3 folder.

        s3.put_object(Bucket = bucket,Key = destination,Body = Csv_buffer.getvalue(),ContentType='text/csv')

        return "Daily_Weather_Forecast file has been successfully uploaded.\n"

    
    Current_Weather_Status = Transform_Current_Weather()
    
    Hourly_Weather_Forecast_status = Transform_Hourly_Weather_Forecast()
    
    Daily_Weather_Forecast_status = Transform_Daily_Weather_Forecast()
    
    return print(Current_Weather_Status,Hourly_Weather_Forecast_status,Daily_Weather_Forecast_status)

status = Transformation()
print(status)