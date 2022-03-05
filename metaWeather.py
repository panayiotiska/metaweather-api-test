import requests
import time
import pandas as pd
import sqlite3

# Examples
# First give a location as string to get the woeid of that locationn
#https://www.metaweather.com/api/location/search/?query=London
# Then use that woied to get the weather data
#https://www.metaweather.com/api/location/44418/

API_ROOT = 'https://www.metaweather.com'
API_LOCATION = '/api/location/search/?query='  # + city
API_WEATHER = '/api/location/'  # + woeid

def store_data(weather_data, city):
    weather_dataframe = None
    
    for i in range(len(weather_data)):
        date = weather_data[i]['applicable_date']
        state = weather_data[i]['weather_state_name']
        max_temp = weather_data[i]['max_temp']
        max_temp = round(max_temp, 2)
        min_temp = weather_data[i]['min_temp']
        min_temp = round(min_temp, 2)
        avg_temp = weather_data[i]['the_temp']
        avg_temp = round(avg_temp, 2)
        wind_speed = weather_data[i]['wind_speed']
        humidity = weather_data[i]['humidity']

        # Create dataframe
        weather_dataframe_row = pd.DataFrame([[city,date,state,max_temp,min_temp,avg_temp,wind_speed,humidity]],columns=['city','date','state','max_temp','min_temp','avg_temp','wind_speed','humidity'])
        weather_dataframe = pd.concat([weather_dataframe, weather_dataframe_row])
    
        #print(f" {date} \t {state} \t High: {max_temp}°C \t Low: {min_temp}°C \t Avg: {avg_temp}°C")

    # Insert a row of data
    for index, row in weather_dataframe.iterrows():
        cur.execute(f"INSERT INTO MetaWeather VALUES ('{row['city']}','{row['date']}','{row['state']}',{row['max_temp']},{row['min_temp']},{row['avg_temp']},{row['wind_speed']},{row['humidity']})")
        con.commit() # Save (commit) the changes

    print(weather_dataframe)


def weather_report(cities):
    for city in cities:
        try:
            print('---------------------\n')
            r1 = requests.get(API_ROOT + API_LOCATION + city)
            # if r1.status_code == 200:
            location_data = r1.json()
            woeid = location_data[0]['woeid']
            print('Location Data Fetched successfully...')

            r2 = requests.get(API_ROOT + API_WEATHER + str(woeid))
            print('Getting Weather Data, Please wait...')
            print('Weather Data of ' + city.capitalize() + ':')
            # We will get a dictionary having keys: consolidated_weather, time, sun_rise, sun_set, timezone_name, parent, sources, title, location_type, woeid, latt_long, timezone.
            weather_data = r2.json()['consolidated_weather']
            # We will get 6 day Weather Forecast data in weather data as a list of dictionaries.
            # Each day having keys: id,weather_state_name, weather_state_abbr, wind_direction_compass, created, applicable_date, min_temp, max_temp, the_temp, wind_speed, wind_direction, air_pressure, humidity, visibility, predictability
            store_data(weather_data, city)

        except requests.exceptions.ConnectionError:
            print('Sorry there is a network error')

        except Exception as e:
            print("There was an unexpected error.\n")
            print(e)
            #weather_report(cities)


if __name__ == '__main__':

    # Write to SQLITE
    con = sqlite3.connect('metaweather.db')
    cur = con.cursor()

    # Create table
    cur.execute('''CREATE TABLE MetaWeather
                (city text, date text, state text, max_temp double, min_temp double, avg_temp double, wind_speed double, humidity double)''')

    # Call function
    three_cities = ['London', 'Dublin', 'Cardiff']
    weather_report(three_cities)

    # Close connection
    con.close()