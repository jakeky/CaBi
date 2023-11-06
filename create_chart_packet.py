# -*- coding: utf-8 -*-
"""
Created on Sun Aug 20 13:46:38 2023

@author: jacob
"""
import sys
sys.path.append(r'C:\Users\jacob\OneDrive\Clubs\Bike Falls Church\Capital Bikeshare')
import cb
import pandas as pd
from datetime import datetime, date

chart_path = r'C:\Users\jacob\OneDrive\Clubs\Bike Falls Church\Capital Bikeshare'
data_path = r'C:\Users\jacob\Downloads\capital_bikeshare_data'

# Stations that have changed locations in Falls Church

# Only two start locations for the first entry
# 31948
# W&OD Trail bridge & N Washington St
# W&OD Trail & Langston Blvd

# 32600
# May 2019 to Nov. 2022
# George Mason High School / Haycock Rd & Leesburg Pike
# Meridian High School / Haycock Rd & Leesburg Pike
# Dec. 2022 to present
# Founders Row/W Broad St & West St

# 32601
# State Theatre / Park Pl & N Washington St
# June 2022 to present
# Eden Center

# Choose start or end
mode = 'end'

upd_date =  date.today().strftime("%Y-%m-%d")
end_chart_date = '2023-12-31'

df_long = pd.read_csv(f'{data_path}\{mode}_data.csv')

station_names_by_id = {31948:'W&OD Trail & Langston Blvd',
               32600:'Founders Row/W Broad St & West St',
               32601:'Eden Center'
               }

# Only keep values for current station location
for key, value in station_names_by_id.items():
    df_long = df_long.query(f"start_station_id != @key or {mode}_station_name == @value")

df_long = df_long.drop_duplicates()

df_long['trips'] = 1

# Convert the string index to a monthly period
df_long['month_date'] = pd.to_datetime(df_long['started_at']).dt.to_period('M')

df_long = df_long[['month_date', f'{mode}_station_id', 'trips']]

df_sum = df_long.groupby([f'{mode}_station_id', 'month_date'])['trips'].sum()

df_sum = df_sum.reset_index()

df_wide = df_sum.pivot(index='month_date', columns=f'{mode}_station_id', values='trips')

# Loop over all the columns in the dataframe
for col in df_wide.columns:
    # Save each column as a separate series
    # Use the format series_col_name for the new series name
    # Use the globals() function to create a global variable with the new series name
    globals()['series_' + str(col)] = df_wide[col]
###############################################################################
# Exhibit 1
###############################################################################
first_exhibit = cb.Exhibit([3,3], normal_font = r'C:\Users\jacob\OneDrive\Program Languages\Python\texgyreheros\texgyreheros-regular.otf', bold_font = r'C:\Users\jacob\OneDrive\Program Languages\Python\texgyreheros\texgyreheros-bold.otf', 
                     italic_font = r'C:\Users\jacob\OneDrive\Program Languages\Python\texgyreheros\texgyreheros-italic.otf', bold_italic_font = r'C:\Users\jacob\OneDrive\Program Languages\Python\texgyreheros\texgyreheros-bolditalic.otf', h_space = .4) #h_space argument adds vertical space between charts to make room for footnotes
first_exhibit.add_exhibit_title("Capital Bikeshare in Falls Church\n(2019 to present)")

mon = datetime.today().strftime("%b.").replace("May.", "May").replace("Jun.", "June").replace("Jul.", "July").replace("Sep.", "Sept.")

first_exhibit.add_exhibit_captions('Jacob Williams\njacob@wescinc.com', datetime.today().strftime(f'{mon} %d, %Y'))
###############################################################################
# Panel 1
###############################################################################
first_exhibit.add_panel_ts("panel1", ["2019-03-01", end_chart_date], 0, 0, v_end = 3)
first_exhibit.add_panel_title("panel1", 'Monthly Total Rides at East Falls Church Metro station (Arlington County)')
#first_exhibit.add_panel_captions("panel1", 'Monthly', 'Rides')

first_exhibit.plot_panel_ts_line("panel1", series_31904, line_color = '#69369E')

first_exhibit.format_panel_numaxis("panel1", num_range = [0, 400], tick_pos = range(50, 400, 50))
first_exhibit.format_panel_ts_xaxis("panel1", mark_years=True, major_pos = cb.gen_ts_tick_label_range("2019-03-01", f'2023-09-01', "A"),
                                    label_dates = cb.gen_ts_tick_label_range("2019-01-01", end_chart_date, "A", skip=1), label_fmt='%Y')

last_date = last_date = series_31904.index.max()
last_date_fmt_month = cb.format_month_irregular(last_date)
last_date_fmt = last_date_fmt_month + last_date.strftime(' %Y')

first_exhibit.add_panel_footnotes("panel1", [f'Note: Data from May 2019 to {last_date_fmt}.\nSource: Capital Bikeshare.'], y_pos = -.08)

#first_exhibit.add_panel_keylines("panel1", "2019-07-20", 200, ["East Falls Church"], color_list = ['#69369E', '#656565'])
###############################################################################
# Panel 2
###############################################################################
first_exhibit.add_panel_ts("panel2", ["2019-03-01", end_chart_date], 1, 0, v_end = 3)
first_exhibit.add_panel_title("panel2", 'Monthly Total Rides at Falls Church Stations')
#first_exhibit.add_panel_captions("panel1", 'Monthly', 'Rides')

first_exhibit.plot_panel_ts_line("panel2", series_31948, line_color = '#828282')
first_exhibit.plot_panel_ts_line("panel2", series_32232, line_color = '#9B870C')
first_exhibit.plot_panel_ts_line("panel2", series_32600, line_color = '#69369E')
first_exhibit.plot_panel_ts_line("panel2", series_32603, line_color = '#498854')
first_exhibit.plot_panel_ts_line("panel2", series_32605, line_color = '#6D241E')
first_exhibit.plot_panel_ts_line("panel2", series_32607, line_color = '#589BB2')

first_exhibit.format_panel_numaxis("panel2", num_range = [0, 200], tick_pos = range(0, 200, 20))
first_exhibit.format_panel_ts_xaxis("panel2", mark_years=True, major_pos = cb.gen_ts_tick_label_range("2019-03-01", f'2023-09-01', "A"),
                                    label_dates = cb.gen_ts_tick_label_range("2019-01-01", end_chart_date, "A", skip=1), label_fmt='%Y')

first_exhibit.add_panel_footnotes("panel2", [f'Note: Data from May 2019 to {last_date_fmt}.\nSource: Capital Bikeshare.'], y_pos = -.08)

first_exhibit.add_panel_keylines("panel2", "2019-05-20", 170, ['W&OD Trail & Langston Blvd (Arlington County)',
                                                               'West Falls Church (Fairfax County)',
                                                               'Founders Row/W Broad St & West St',
                                                               'Pennsylvania Ave & Park Ave',
                                                               'W Broad St & Little Falls St',
                                                               'S Maple Ave & S Washington St',
                                                               ]
                                 , color_list = ['#828282', 
                                                 '#9B870C',
                                                 '#69369E',
                                                 '#498854',
                                                 '#6D241E', 
                                                 '#589BB2'
                                                 ])
###############################################################################
# Panel 3
###############################################################################
first_exhibit.add_panel_ts("panel3", ["2019-03-01", end_chart_date], 2, 0, v_end = 3)
first_exhibit.add_panel_title("panel3", 'Monthly Total Rides at Falls Church Stations')
#first_exhibit.add_panel_captions("panel1", 'Monthly', 'Rides')

first_exhibit.plot_panel_ts_line("panel3", series_32601, line_color = '#828282')
first_exhibit.plot_panel_ts_line("panel3", series_32602, line_color = '#9B870C')
first_exhibit.plot_panel_ts_line("panel3", series_32604, line_color = '#69369E')
first_exhibit.plot_panel_ts_line("panel3", series_32606, line_color = '#498854')
first_exhibit.plot_panel_ts_line("panel3", series_32608, line_color = '#6D241E')
first_exhibit.plot_panel_ts_line("panel3", series_32609, line_color = '#589BB2')

first_exhibit.format_panel_numaxis("panel3", num_range = [0, 200], tick_pos = range(0, 200, 20))
first_exhibit.format_panel_ts_xaxis("panel3", mark_years=True, major_pos = cb.gen_ts_tick_label_range("2019-03-01", f'2023-09-01', "A"),
                                    label_dates = cb.gen_ts_tick_label_range("2019-01-01", end_chart_date, "A", skip=1), label_fmt='%Y')

first_exhibit.add_panel_footnotes("panel3", [f'Note: Data from May 2019 to {last_date_fmt}.\nSource: Capital Bikeshare.'], y_pos = -.08)

first_exhibit.add_panel_keylines("panel3", "2019-05-20", 190, ['Eden Center',
                                                               'N Oak St & W Broad St',
                                                               'E Fairfax St & S Washington St',
                                                               'N Roosevelt St & Roosevelt Blvd',
                                                               'Falls Church City Hall / Park Ave & Little Falls St',
                                                               'W Columbia St & N Washington St'
                                                               ]

                                 , color_list = ['#828282', 
                                                 '#9B870C',
                                                 '#69369E',
                                                 '#498854',
                                                 '#6D241E', 
                                                 '#589BB2'
                                                 ])
###############################################################################
first_exhibit.save_exhibit(fr'{chart_path}\{upd_date}_{mode}_falls_church_cabi_ridership.pdf')
