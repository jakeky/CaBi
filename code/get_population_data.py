# -*- coding: utf-8 -*-
"""
Created on Sun Aug 20 00:34:21 2023

@author: jacob
"""

# Fetch population counts for the Capital Bikeshare jurisdictions: 
# Washington, DC; Arlington, VA; Alexandria, VA; Montgomery County, MD; 
# Prince George's County, MD; Fairfax County, VA; and the City of Falls Church, VA.

import os
import ast
import pandas as pd

os.chdir(r'C:\Users\jacob\OneDrive\Clubs\Bike Falls Church\Capital Bikeshare')

# Define a list of FIPS codes for the places we want to query
# The FIPS codes are composed of state code (2 digits) and place code (5 digits)
fips_codes = ["11000", "51013", "51510", "24031", "24033", "51059", "51610"]

# Census API key
key = put your own key here

# Make a loop to iterate over the FIPS codes and append the API responses to a list
data_list = []
for fips in fips_codes:
    print(fips)
    # Split the fips code into state and place codes
    state = fips[:2]
    county = fips[2:]
    # Make the API request and get the JSON response
    # Get total population
    #os.system(f'curl -o out.txt "https://api.census.gov/data/2020/dec/dhc?get=NAME,P1_001N&for=county:{county}&in=state:{state}&key={key}" --ssl-no-revoke')
    print(f'curl -o {fips}.txt "https://api.census.gov/data/2020/dec/dhc?get=NAME,P1_001N&for=county:{county}&in=state:{state}&key={key}"')
         
    # DC: f'curl -o {fips}.txt "https://api.census.gov/data/2020/dec/dhc?get=NAME,P1_001N&for=state:11&key=4c059df1009db1beb1c1894be6853393a68ca03a"'
    
    # Open the text file in read mode
    with open(f'{fips}.txt', 'r') as f:
        # Read the file content as a string
        content = f.read()

        content = ast.literal_eval(content)[1]
        
        # Get the first and second items from the second list
        name = content[0]
        pop = int(content[1])
    
    data_list.append([name, pop])

# Convert the list of responses to a pandas dataframe
df = pd.DataFrame(data_list, columns=['place','pop_2020'])

df.to_csv('population_by_place.csv',index=False)
