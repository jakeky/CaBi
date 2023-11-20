# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
import os
import json
import pandas as pd
import matplotlib.pyplot as plt
import sys
sys.path.append('/home/jmw10/CaBi')
import cb
import datetime
import imageio
import pytz
from glob import glob
    
upd_date =  datetime.date.today().strftime("%Y-%m-%d")
end_chart_date = '2023-11-15'

def get_day_of_week(year, month, day):
    # Create a datetime object from the given year, month, and day
    date = datetime.datetime(year, month, day)

    # List of days in a week
    days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

    # Get the day of the week as an integer
    day_of_week_num = date.weekday()

    # Return the corresponding day name
    return days[day_of_week_num]

stations = {
    31904: "East Falls Church Metro (Arlington)",
    31948: "W&OD Trail & Langston Blvd (Arlington)",
    32232: "West Falls Church Metro (Fairfax)",
    32600: "Founders Row/W Broad St & West St",
    32601: "Eden Center",
    32602: "N Oak St & W Broad St",
    32603: "Pennsylvania Ave & Park Ave",
    32604: "E Fairfax St & S Washington St",
    32605: "W Broad St & Little Falls St",
    32606: "N Roosevelt St & Roosevelt Blvd",
    32607: "S Maple Ave & S Washington St",
    32608: "Falls Church City Hall / Park Ave, & Little Falls St",
    32609: "W Columbia St & N Washington St",
    31125: "15th & W St NW"
}

short_ids = ['31904', '31948', '32232', '32600', '32601', '32602', '32603', '32604', '32605', '32606', '32607', '32608', '32609',  '31125']

path = '/home/jmw10/CaBi/'

with open(f'{path}station_information.json') as f:
    d = json.load(f)
    
# Load the json data into a dictionary
data = d['data']['stations']

# Convert the dictionary into a dataframe
station_info = pd.DataFrame(data)

target_station_info = station_info.query('short_name in @short_ids')

station_ids = target_station_info['station_id'].values.tolist()

capacity_by_station = target_station_info[['station_id', 'capacity', 'short_name']]
###############################################################################
json_files = glob(f'{path}*station_status.json')
json_files.sort()

dfs = []
for file in json_files:
    print(file)
    with open(file) as f:
        d = json.load(f)
    
    # Load the json data into a dictionary
    data = d['data']['stations']
    
    # Convert the dictionary into a dataframe
    df = pd.DataFrame(data)

    target_stations = df.query('station_id in @station_ids')
    
    dfs.append(target_stations)
    
df = pd.concat(dfs,axis='index')

df = df.drop_duplicates(subset=['last_reported','station_id'])

# The last_reported column is in unix epoch time
df['date_time_utc'] = pd.to_datetime(df['last_reported'], unit='s') # unit='s' for seconds or 'ms' for milliseconds

# Convert UTC to Eastern Time
eastern = pytz.timezone('US/Eastern')
df['date_time'] = df['date_time_utc'].dt.tz_localize('UTC').dt.tz_convert(eastern)

# The current datetime is at the end of the hour so associate with the next hour
df['date_time'] = df['date_time'] + datetime.timedelta(hours=1)

# Do summary stats
df.describe()

# Apply the function to the datetime column and create a new column with the hour range
# Add one hour since the recorded time is at the end of the hour
df["hour_range"] = df["date_time"].dt.hour + 1

df = df.merge(capacity_by_station, on='station_id')

# Check for data issue
impossible = df.query('capacity < num_bikes_available')

df['dock_utilization_rate'] = df['num_bikes_available'] / df['capacity'] * 100

df = df.set_index('date_time')

df = df[['short_name', 'dock_utilization_rate']]

df_wide = df.pivot(columns='short_name', values='dock_utilization_rate')

# Loop over all the columns in the dataframe
for col in df_wide.columns:
    # Save each column as a separate series
    # Use the format series_col_name for the new series name
    # Use the globals() function to create a global variable with the new series name
    globals()['series_' + str(col)] = df_wide[col].dropna()
    
for series_id in short_ids: 
    series = df_wide[series_id].dropna()
    ###############################################################################
    # Exhibit 1
    ###############################################################################
    first_exhibit = cb.Exhibit([3,3], normal_font = '/home/jmw10/CaBi/texgyreheros/texgyreheros-regular.otf', bold_font = '/home/jmw10/CaBi/texgyreheros/texgyreheros-bold.otf', 
                         italic_font = '/home/jmw10/CaBi/texgyreheros/texgyreheros-italic.otf', bold_italic_font = '/home/jmw10/CaBi/texgyreheros/texgyreheros-bolditalic.otf', h_space = .4) #h_space argument adds vertical space between charts to make room for footnotes
    first_exhibit.add_exhibit_title("Capital Bikeshare in Falls Church")
    
    mon = datetime.date.today().strftime("%b.").replace("May.", "May").replace("Jun.", "June").replace("Jul.", "July").replace("Sep.", "Sept.")
    
    first_exhibit.add_exhibit_captions('Jacob Williams\njacob@wescinc.com', datetime.date.today().strftime(f'{mon} %#d, %Y'))
    ###############################################################################
    # Panel 1
    ###############################################################################
    first_exhibit.add_panel_ts("panel1", ["2023-08-25", end_chart_date], 0, 0, v_end = 3)
    title = stations[int(series_id)]
    first_exhibit.add_panel_title("panel1", f'Percentage of Available Bikes at {title} Station')
    #first_exhibit.add_panel_captions("panel1", 'Monthly', 'Rides')
    
    first_exhibit.plot_panel_ts_line("panel1", series, line_color = '#69369E')
    
    first_exhibit.format_panel_numaxis("panel1", num_range = [0, 100], tick_pos = range(0, 100, 20))
    first_exhibit.format_panel_ts_xaxis("panel1", mark_years=True, major_pos = cb.gen_ts_tick_label_range("2023-08-25", end_chart_date, "M"),
                                        label_dates = cb.gen_ts_tick_label_range("2023-08-25", end_chart_date, "M"), label_fmt='%b.')
    
    start_date = series.index.min()
    start_date_fmt_month = cb.format_month_irregular(start_date)
    start_date_fmt = start_date_fmt_month + start_date.strftime(' %Y')
    
    last_date = series.index.max()
    last_date_fmt_month = cb.format_month_irregular(last_date)
    last_date_fmt = last_date_fmt_month + last_date.strftime(' %Y')
    
    first_exhibit.add_panel_footnotes("panel1", [f'Note: Data from {start_date_fmt} to {last_date_fmt}. The data indicate the bike supply available at the start of each hour.\nSource: Capital Bikeshare and author\'s calculations.'], y_pos = -.08)
    
    first_exhibit.save_exhibit(f'{path}/{upd_date}_bike_supply_{series_id}.pdf')

pdfs = glob(f'{upd_date}*.pdf')
pdfs.sort()

for pdf in pdfs:
    os.system(f'pdfcrop {pdf} {pdf}')

os.system(f'pdftk {upd_date}_bike_supply_*.pdf cat output {upd_date}_cabi_bike_supply_falls_church.pdf')

###############################################################################
df_31125 = series_31125.to_frame()

df_31125 = df_31125.reset_index()

# Apply the function to the datetime column and create a new column with the hour range
df_31125["hour_range"] = df_31125["date_time"].dt.hour

df_hour_range = df_31125.groupby('hour_range')['31125'].mean()

# Reindex the series with a new index from 0 to 24 and fill the missing values with 0
df_hour_range = df_hour_range.reindex(range(25), fill_value=0)

hours = df_hour_range.index.values.tolist()

hour_vals = df_hour_range.values.tolist()

# Convert the int64 index to a datetime object with the unit of hours
hours = pd.to_datetime(hours, unit="h")

# Format the datetime object as a string with the 12 hour format and am/pm
hours = hours.strftime("%-I %p")

# Create a figure with 300 dpi
plt.figure(dpi=300)

# Create a bar chart with the months as the x-axis and the values as the y-axis
plt.bar(hours,hour_vals,color='#69369E')

# Add a title and labels for the axes
plt.title('Percentage of Available Bikes at 15th & W St NW Station')
plt.xlabel("Hour of day")
plt.yticks([0,20,40,60,80,100]) # set the y-axis ticks
plt.xticks([0,5,9,13,17,21]) # set the x-axis ticks
#plt.ylabel("Values")

# Add a source note to the bar chart
plt.figtext(0.12, -0.05, "Note: The data represent hourly averages, spanning from Aug. 2023\nto Nov. 2023, and indicate the bike supply available at the start of each hour.\nSource: Capital Bikeshare and author's calculations.")

# Remove the top x-axis tick marks
plt.tick_params(axis='x', top=False)

# Adjust the subplot parameters to give more space between the footnote and the x-axis label
plt.subplots_adjust(bottom=0.12)  # Adjust the bottom spacing

plt.savefig(f'{path}hour_of_day_average_pct_avail.png', bbox_inches='tight', pad_inches=0.5)

# Show the plot
plt.show()

df_31125['day'] = df_31125['date_time'].dt.date

# Loop over each unique day
for day in df_31125['day'].unique():
    print(f"Processing {day}")
    # You can perform your operation here
    # For example, filter the DataFrame for the current day
    filtered_df = df_31125[df_31125['day'] == day]

    df_hour_range = filtered_df.groupby('hour_range')['31125'].mean()

    # Reindex the series with a new index from 0 to 24 and fill the missing values with 0
    df_hour_range = df_hour_range.reindex(range(25), fill_value=0)

    hours = df_hour_range.index.values.tolist()

    hour_vals = df_hour_range.values.tolist()

    # Convert the int64 index to a datetime object with the unit of hours
    hours = pd.to_datetime(hours, unit="h")

    # Format the datetime object as a string with the 12 hour format and am/pm
    hours = hours.strftime("%-I %p")

    # Create a figure with 300 dpi
    plt.figure(dpi=300)

    # Create a bar chart with the months as the x-axis and the values as the y-axis
    plt.bar(hours,hour_vals,color='#69369E')

    day_fmt_month = cb.format_month_irregular(day)
    day_fmt = day_fmt_month + day.strftime(' %-d, %Y')
    
    day_name = get_day_of_week(day.year, day.month, day.day)

    # Add a title and labels for the axes
    plt.title(f'Percentage of Available Bikes at 15th & W St NW Station\n{day_fmt} - {day_name}')
    plt.xlabel("Hour of day")
    plt.yticks([0,20,40,60,80,100]) # set the y-axis ticks
    plt.xticks([0,5,9,13,17,21]) # set the x-axis ticks
    #plt.ylabel("Values")

    # Add a source note to the bar chart
    plt.figtext(0.12, -0.05, "Note: The data indicate the bike supply available at the start of each hour.\nSource: Capital Bikeshare and author's calculations.")

    # Remove the top x-axis tick marks
    plt.tick_params(axis='x', top=False)

    # Adjust the subplot parameters to give more space between the footnote and the x-axis label
    plt.subplots_adjust(bottom=0.12)  # Adjust the bottom spacing

    plt.savefig(f'{path}{day}_plot.png', bbox_inches='tight', pad_inches=0.5)

    # Show the plot
    plt.show()

# Get all PNG files in the folder
png_files = glob(f'{path}*-*-*_plot.png')

# Sort files if needed
png_files.sort()

# Create a list to hold the images
images = []

# Loop through all PNG files and add them to the images list
for file in png_files:
    images.append(imageio.imread(file))

# Save the images as a gif
gif_path = os.path.join(path, 'bike_availibility_per_day.gif')
imageio.mimsave(gif_path, images, duration=1)  # duration is in seconds per frame
