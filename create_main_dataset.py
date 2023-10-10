# -*- coding: utf-8 -*-
"""
Created on Sat Aug 19 12:30:25 2023
@author: jacob
"""
# The code reads, processes, and writes data from Capital Bikeshare, a bike-sharing service in Washington D.C. and 
# its surrounding areas. It imports the necessary modules, creates a dictionary to map the old and new column names,
# defines a function to rename the columns based on the file date, gets a list of CSV files, reads them as Dask dataframes,
# assigns a file date column, drops duplicate rows, applies the rename function, appends the dataframes, 
# writes the output to a parquet file, and includes a comment about the column name change.

# Starting with the 202004 file, the fields change from

#['Duration', 'Start date', 'End date', 'Start station number', 'Start station', 
# 'End station number', 'End station', 'Bike number', 'Member type']

# to 

# ['ride_id', 'rideable_type', 'started_at', 'ended_at', 'start_station_name', 
# 'start_station_id', 'end_station_name', 'end_station_id', 'start_lat', 'start_lng',
#  'end_lat', 'end_lng', 'member_casual']

import os
import pandas as pd
import dask.dataframe as dd
from glob import glob
from dask.diagnostics import ProgressBar
from dask_sql import Context

main_dir = r'C:\Users\jacob\Downloads\capital_bikeshare_data'

# Create a dictionary that maps the old column names to the new column names
column_map = {'Start date': 'started_at', 'End date': 'ended_at', 
              'Start station number': 'start_station_id', 'Start station': 'start_station_name', 
              'End station number': 'end_station_id', 'End station': 'end_station_name', 
              'Member type': 'member_casual'}

# Define a function that takes a dataframe and 
# a file date as arguments and returns a renamed dataframe
def rename_columns(df, file_date):
    # Convert the file date to a datetime object using the format '%Y'
    if len(file_date) == 6 and 'Q' in file_date:
        format_code = None
    elif len(file_date) == 6 and 'Q' not in file_date:
        format_code = '%Y%m'
    elif len(file_date) == 4:
        format_code = '%Y'
    file_date = pd.to_datetime(file_date.replace('Q', '-Q'), format=format_code)
    # Define a cutoff date as March 2020
    cutoff_date = pd.to_datetime('202003', format='%Y%m')
    # Check if the file date is before or equal to the cutoff date
    if file_date <= cutoff_date:
        # Use the df.rename() function to replace the old column names with the new column names
        df = df.rename(columns=column_map)
    # Return the renamed dataframe
    return df

# Use the glob function to get a list of all the CSV files in the main directory
cabi_files = glob(os.path.join(main_dir, 'csv', '*.csv'))

# Use a list comprehension to loop through each CSV file,
# read it as a Dask dataframe, assign a file date column based
# on the file name, drop any duplicate rows, and apply the
# rename function to it
# Store the resulting dataframes in a list named cabi_dfs

cabi_dfs = [rename_columns(dd.read_csv(csv, engine='python', dtype='object')
               .assign(file_date=csv.split('\\')[-1].split('-')[0])
               .drop_duplicates(), csv.split('\\')[-1].split('-')[0])
               for csv in cabi_files]

# Append all rows
cabi_df = dd.concat(cabi_dfs, axis='index')

# Drop duplicates
cabi_df = cabi_df.drop_duplicates()

# Define a function that takes a column name and returns a modified column name
def modify_column_name(col): 
    # Lowercase the column name and replace spaces with underscores 
    col = col.lower().replace(' ', '_') 
    # Return the modified column name 
    return col

# Apply the function to all column names using the map method
cabi_df.columns = cabi_df.columns.map(modify_column_name)

# Create a context to register tables
c = Context()

# Register the dataframe as a table in the context
c.create_table('cabi_trips', cabi_df)

# Example SQL query
result = c.sql("SELECT * FROM cabi_trips WHERE start_station_id IN ('31208', '31904', '31948', '32232', '32600', '32601', '32602', '32603', '32604', '32605', '32606', '32607', '32608', '32609') AND CAST(started_at AS DATE) > '2019-05-01'")

# Use the ProgressBar() as a context manager around the compute() call
with ProgressBar():
    cut = result.compute()

# Save the dask dataframe to a csv file using the to_csv() function
cut.to_csv('data.csv', index=False)