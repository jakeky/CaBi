# CaBi
This is a GitHub repo with scripts to analyze Capital Bikeshare data. The data has info on bike trips in the D.C. area. The scripts use Python and Dask to read, clean, transform, and query the data with SQL. The repo shows how to use Dask and Dask SQL for large datasets. The repo also has some query and visualization examples.

<p align="center">
  <img src="bike_dc_pic.jpg" width="350">
</p>

## File Descriptions

- `README.md`: Provides an overview and instructions for using the files in the repository.

- `bike_dc_pic.jpg`: An image of a Capital Bikeshare bike against the DC landscape.

- `create_chart_packet.py`: A Python script for generating a set of charts from the data.

- `create_main_dataset.py`: A Python script for creating the main dataset used in the analysis.

- `create_main_dataset_by_population.py`: This script likely refines the main dataset by including population data.

- `download_cabi_data.py`: A script for downloading Capital Bikeshare (CaBi) data.

- `get_population_data.py`: A Python script for retrieving population data to be merged with the main dataset.

- `get_stations_by_location.py`: This script may be used to generate a dataset of bike-sharing stations categorized by location.

- `fcc_bikeshare_monthly_totals_by_end_station.csv`: A CSV file containing monthly total data categorized by the end station of bike trips.

- `fcc_bikeshare_monthly_totals_by_start_station.csv`: A CSV file containing monthly total data categorized by the start station of bike trips.

## Usage

Each script can be run independently to perform the task described by its filename. Ensure that you have the necessary Python environment and dependencies installed.

## Contributing

Contributions to this project are welcome. Please refer to the CONTRIBUTING.md file for guidelines.

## License

This project is licensed under the MIT License - see the LICENSE.md file for details.
