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
import sys
sys.path.append(r'C:\Users\jacob\OneDrive\Clubs\Bike Falls Church\Capital Bikeshare')
import cb
import os
import pandas as pd
import dask.dataframe as dd
import matplotlib.pyplot as plt
import datetime
from glob import glob
from dask.diagnostics import ProgressBar
from dask_sql import Context

upd_date =  datetime.date.today().strftime("%Y-%m-%d")
end_chart_date = '2023-10-31'

main_dir = r'C:\Users\jacob\Downloads\capital_bikeshare_data'

path = r'C:\Users\jacob\OneDrive\Clubs\Bike Falls Church\Capital Bikeshare'

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
c.create_table('df1', cabi_df)

stations_by_location = pd.read_csv('stations_by_location.csv')[['short_name','jurisdiction']]
stations_by_location['short_name'] = stations_by_location['short_name'].astype(str)
stations_by_location = stations_by_location.rename(columns={'short_name':'start_station_id'})

# Register df2 as a temporary table in the sql context
c.create_table('df2', stations_by_location)

# Write a sql query to perform the merge, create the new field, and aggregate the result
query = """
SELECT df1.started_at, df2.jurisdiction, COUNT(df1.start_station_id) AS sum_of_rows
FROM df1
JOIN (SELECT DISTINCT start_station_id, jurisdiction FROM df2) AS df2 ON df1.start_station_id = df2.start_station_id
GROUP BY df1.started_at, df2.jurisdiction
"""

# This sql query does the following steps:

# It selects the distinct start_station_id and jurisdiction values from the table df2 and creates a temporary table named df2.
# It joins the table df1 with the temporary table df2 on the condition that they have the same start_station_id value.
# It groups the joined rows by the started_at and jurisdiction columns.
# It counts the number of rows in each group and assigns it to a new column named sum_of_rows.
# It returns the started_at, jurisdiction, and sum_of_rows columns from the grouped rows.
# You can think of this query as a way to find out how many rides started at each station in each jurisdiction on each date.

result = c.sql(query)

# Use the ProgressBar() as a context manager around the compute() call
with ProgressBar():
    cut = result.compute()

# Convert the date column to datetime format
cut['started_at'] = pd.to_datetime(cut['started_at'])

cut['started_at_month'] = cut['started_at'].dt.month
cut['started_at_year'] = cut['started_at'].dt.year

# Group by month and jurisdiction and sum the sum_of_rows column
result = cut.groupby(['started_at_month', 'started_at_year', 'jurisdiction'])['sum_of_rows'].sum().to_frame()

result = result.sort_values(['jurisdiction', 'started_at_year', 'started_at_month'])

result = result.reset_index()

# Create a monthly period date column from the year and month columns
# Use the pd.to_datetime function with a dictionary of year, month and day as input
# Assign day=1 to create the first day of each month
# Use the dt.to_period method with 'M' as frequency to create monthly periods
result['date'] = pd.to_datetime(dict(year=result.started_at_year, month=result.started_at_month, day=1)).dt.to_period('M')

result = result[['date', 'jurisdiction', 'sum_of_rows']]

pop = pd.read_csv(f'{path}\population_by_place.csv')

# Split the column on the comma and keep the part before the comma
# Use the str.split method with maxsplit=1 to limit the number of splits to one
# Use the str[0] method to access the first element of the resulting list
pop['place'] = pop['place'].str.split(',').str[0]

# Proper case the string in the column
# Use the str.title method to capitalize the first letter of each word
pop['place'] = pop['place'].str.title()

pop['place'] = pop['place'].str.replace('District Of Columbia', 'Washington')
pop['place'] = pop['place'].str.replace("Prince George'S County", "Prince George's County")
pop['place'] = pop['place'].str.replace("Alexandria City", "Alexandria")

pop = pop.rename(columns={'place':'jurisdiction'})

merged = result.merge(pop, on='jurisdiction')

merged['rider_rate'] = merged['sum_of_rows'] / merged['pop_2020'] * 1000

merged = merged[['date', 'jurisdiction', 'rider_rate']]

merged.to_csv(rf'{path}\ridership_per_capita', index=False)

# Pivot the grouped dataframe to have city as columns and month as index
df_pivoted = merged.pivot(index='date', columns='jurisdiction', values='rider_rate')

df_pivoted = df_pivoted.reset_index()

period = pd.Period('2019-01', freq='M')
# Subset the dataframe to keep only the rows after 2019
df_subset = df_pivoted[df_pivoted['date'] > period]

df_subset = df_subset.set_index('date')

df_subset.columns = [col.replace(' ','_').replace("'",'') for col in df_subset.columns]

# Loop over all the columns in the dataframe
for col in df_subset.columns:
    # Save each column as a separate series
    # Use the format series_col_name for the new series name
    # Use the globals() function to create a global variable with the new series name
    globals()['series_' + str(col)] = df_subset[col]
    
###############################################################################
# Exhibit 1
###############################################################################
first_exhibit = cb.Exhibit([3,3], normal_font = r'C:\Users\jacob\OneDrive\Program Languages\Python\texgyreheros\texgyreheros-regular.otf', bold_font = r'C:\Users\jacob\OneDrive\Program Languages\Python\texgyreheros\texgyreheros-bold.otf', 
                     italic_font = r'C:\Users\jacob\OneDrive\Program Languages\Python\texgyreheros\texgyreheros-italic.otf', bold_italic_font = r'C:\Users\jacob\OneDrive\Program Languages\Python\texgyreheros\texgyreheros-bolditalic.otf', h_space = .4) #h_space argument adds vertical space between charts to make room for footnotes
first_exhibit.add_exhibit_title("Capital Bikeshare Ridership\n(2019 to present)")

mon = datetime.date.today().strftime("%b.").replace("May.", "May").replace("Jun.", "June").replace("Jul.", "July").replace("Sep.", "Sept.")

first_exhibit.add_exhibit_captions('Jacob Williams\njacob@wescinc.com', datetime.date.today().strftime(f'{mon} %#d, %Y'))
###############################################################################
# Panel 1
###############################################################################
first_exhibit.add_panel_ts("panel1", ["2019-03-01", end_chart_date], 0, 0, v_end = 3)
first_exhibit.add_panel_title("panel1", 'Capital Bikeshare Ridership by Jurisdiction (per 1,000 population)')
#first_exhibit.add_panel_captions("panel1", 'Monthly', 'Rides')

first_exhibit.plot_panel_ts_line("panel1", series_Washington, line_color = '#828282')
first_exhibit.plot_panel_ts_line("panel1", series_Arlington_County, line_color = '#9B870C')
first_exhibit.plot_panel_ts_line("panel1", series_Alexandria, line_color = '#69369E')
first_exhibit.plot_panel_ts_line("panel1", series_Falls_Church_City, line_color = '#498854')
first_exhibit.plot_panel_ts_line("panel1", series_Montgomery_County, line_color = '#6D241E')
first_exhibit.plot_panel_ts_line("panel1", series_Fairfax_County, line_color = '#589BB2')
first_exhibit.plot_panel_ts_line("panel1", series_Prince_Georges_County, line_color = '#000000')


first_exhibit.format_panel_numaxis("panel1", num_range = [0, 600], tick_pos = range(0, 600, 100))
first_exhibit.format_panel_ts_xaxis("panel1", mark_years=True, major_pos = cb.gen_ts_tick_label_range("2019-03-01", f'2023-09-01', "A"),
                                    label_dates = cb.gen_ts_tick_label_range("2019-01-01", end_chart_date, "A", skip=1), label_fmt='%Y')

last_date = last_date = series_Alexandria.index.max()
last_date_fmt_month = cb.format_month_irregular(last_date)
last_date_fmt = last_date_fmt_month + last_date.strftime(' %Y')

first_exhibit.add_panel_footnotes("panel1", [f'Note: Data from May 2019 to {last_date_fmt}.\nSource: Capital Bikeshare; Census Bureau; OpenStreetMap.'], y_pos = -.08)

first_exhibit.add_panel_keylines("panel1", "2020-01-20", 600, ['Washington, D.C.',
                                                               'Arlington County',
                                                               'Alexandria',
                                                               'Falls Church City',
                                                               'Montgomery County',
                                                               'Fairfax County',
                                                               'Prince George\'s County'
                                                               ]
                                 , color_list = ['#828282', 
                                                 '#9B870C',
                                                 '#69369E',
                                                 '#498854',
                                                 '#6D241E', 
                                                 '#589BB2',
                                                 '#000000'
                                                 ])
###############################################################################
first_exhibit.save_exhibit(fr'{chart_path}\{upd_date}_cabi_ridership_per_capita.pdf')
