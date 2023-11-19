# -*- coding: utf-8 -*-
"""
Created on Sat Aug 19 16:51:36 2023

@author: jacob
"""
# Fetch county or county-equivalent for each Capital Bikeshare station

import requests
import pandas as pd
import numpy as np
from geopy.geocoders import Nominatim
from time import sleep

path = r'C:\Users\jacob\OneDrive\Clubs\Bike Falls Church\Capital Bikeshare'

# Define a custom function that returns county or city name
def get_county_or_city(lat, lon):
    try:
        print(lat, lon)
        sleep(2)
        # Reverse geocode the coordinates
        location = geolocator.reverse((lat, lon))
        # access the raw JSON data
        data = location.raw
        # Get the address components
        address = data['address']
        # Try to get the county name
        county = address['county']
        # Return the county name
        return county
    except:
        try:
            # If county name is not available, try to get the city name
            city = address['city']
            # Return the city name
            return city
        except:
            # If both county and city names are not available, return None
            return None
        
geolocator = Nominatim (user_agent="my_app_name")

# Station information file with coordinates
data = requests.get('https://gbfs.lyft.com/gbfs/1.1/dca-cabi/en/station_information.json').json()['data']['stations']

df = pd.DataFrame(data)

# Apply the custom function to the lat and long columns using np.vectorize
df['jurisdiction'] = np.vectorize(get_county_or_city)(df['lat'], df['lon'])

df['jurisdiction'] = df['jurisdiction'].replace(['None', 'Fairfax (city)'], 'Falls Church City')

df.jurisdiction.value_counts()

df.jurisdiction.value_counts().sum() # 737 stations

df.to_csv(r'{path}\stations_by_location.csv', index=False)
