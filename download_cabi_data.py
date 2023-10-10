# -*- coding: utf-8 -*-
"""
Created on Sun Aug 27 15:27:07 2023

@author: jacob
"""
# The code is a Python script that downloads and extracts Capital Bikeshare data from a website. 
# Capital Bikeshare is a bike-sharing system that operates in the Washington, D.C. metropolitan area. 
# The data contains information about the trips taken by the users, such as the start and end stations, 
# the duration, the bike number, and the user type.

# The code does the following steps:

# It defines a function to generate a list of month names based on a start and end date.
# It sets some settings, such as whether to download the yearly files or not, and what is the main directory and the base URL for the data.
# It defines a list of prefixes for the files to download, which are either the year or the month in YYYYMM format.
# It loops through the prefixes and constructs the file name and URL for each prefix.
# It downloads and saves the file content as a zip file in a subdirectory named ‘zip’.
# It opens and extracts the zip file to another subdirectory named ‘csv’.
# It fixes a typo in the original csv file name for January 2018 by copying it to a new file name.

import os
import requests
import zipfile
import datetime as dt
import shutil 
import logging
from dateutil.relativedelta import relativedelta 

# Settings

# Flag for whether to download 2010 to 2017 annual files
dl_year_files = False
# Start month of data to download
start_month = '202307'
# End month of data to download
end_month = '202307'

main_dir = r'C:\Users\jacob\Downloads\capital_bikeshare_data'
base_url = 'https://s3.amazonaws.com/capitalbikeshare-data/'

os.chdir(f'{main_dir}')

# Get the current date and time
today_str = dt.datetime.now().strftime('%Y%m%d')

# Create a logger object with a name of your choice
logger = logging.getLogger('my_logger')
# Set the level of the logger to control what messages are logged
logger.setLevel(logging.INFO)
# Create a file handler object with the file name of your choice
file_handler = logging.FileHandler(f'{today_str}_capital_bikeshare_data.log')
# Set the level of the file handler to control what messages are written to the file
file_handler.setLevel(logging.INFO)
# Create a formatter object with the format of your choice
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# Add the formatter to the file handler
file_handler.setFormatter(formatter)
# Add the file handler to the logger
logger.addHandler(file_handler)

# Define a function to generate a list of month names
def get_dates(start, end): 
    # Initialize an empty list 
    dates = [] 
    # Define the month increment 
    month = relativedelta(months=+1) 
    # Define the date format
    fmt ='%Y%m' 
    # Convert the input strings to date objects 
    start_date = dt.datetime.strptime(start, fmt).date() 
    end_date = dt.datetime.strptime(end, fmt).date() 
    # Loop through the dates from start to end 
    d = start_date 
    while d <= end_date: 
        # Append the date to the list in string format
        dates.append(d.strftime(fmt)) 
        # Increment the date by one month 
        d += month
        # Return the list of dates 
        return dates

# Define the list of prefixes for the files
year_prefixes = [str(year) for year in range(2010, 2018)] 
month_prefixes = get_dates(start_month, end_month)

prefixes = year_prefixes if dl_year_files else [] 
prefixes += month_prefixes

# Loop through the prefixes
for prefix in prefixes: 
    # Construct the file name and URL 
    file_name = prefix + '-capitalbikeshare-tripdata.zip'
    file_url = base_url + file_name

    zip_path = os.path.join(main_dir, 'zip', file_name)
    csv_path = os.path.join(main_dir, 'csv')
    
    # Download and save the file content
    file_content = requests.get(file_url).content
    with open(zip_path, 'wb') as f:
        f.write(file_content)
    
    logger.info(f"Downloaded {file_name} successfully.")
    
    # Open and extract the zip file to csv_path directory
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        for file_name in zip_ref.namelist():
            if file_name.endswith('.csv') and '/' not in file_name:
                zip_ref.extract(file_name, csv_path)
                logging.info(f'Extracted {file_name} successfully.')
    
        logger.info(f'Unzipped {zip_path} successfully.')
            
    if prefix == '201801':
        # Fix typo in original csv file name by copying it to a new file name
        shutil.copy(os.path.join(csv_path, prefix + '_capitalbikeshare_tripdata.csv'), os.path.join(csv_path, prefix + '-capitalbikeshare_tripdata.csv'))
