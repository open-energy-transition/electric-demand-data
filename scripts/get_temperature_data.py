import logging
import os

import util.general_utilities as general_utilities
import util.geospatial_utilities as geospatial_utilities
import util.temperature_utilities as temperature_utilities
import util.time_series_utilities as time_series_utilities


def run_temperature_calculation() -> None:
    """
    Run the calculation of the temperature data according to the population density.
    """

    # Set up the logging configuration.
    log_files_directory = general_utilities.read_folders_structure()["log_files_folder"]
    os.makedirs(log_files_directory, exist_ok=True)
    log_file_name = "temperature_data.log"
    logging.basicConfig(
        filename=log_files_directory + "/" + log_file_name,
        level=logging.INFO,
        filemode="w",
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    # Create a directory to store the weather data.
    result_directory = general_utilities.read_folders_structure()["temperature_folder"]
    os.makedirs(result_directory, exist_ok=True)

    # Read the codes of the regions of interest.
    settings_directory = general_utilities.read_folders_structure()["settings_folder"]
    region_codes = general_utilities.read_countries_from_file(
        settings_directory + "/gegis__all_countries.txt"
    )
    # region_codes = general_utilities.read_us_regions_from_file(settings_directory+"/us_eia_regions.txt")

    # Define the target file type.
    file_type = ".parquet"

    # Define the years of the data retrieval.
    start_year = 2015
    end_year = 2015
    years = range(start_year, end_year + 1)

    # Loop over the years.
    for year in years:
        logging.info(f"Year {year}.")

        # Loop over the regions of interest.
        for region_code in region_codes:
            logging.info(f"Retrieving data for {region_code}.")

            # Define the file paths of the temperature time series.
            temperature_file_path_top_1 = (
                result_directory
                + f"/temperature_time_series_top_1_{region_code}_{year}"
                + file_type
            )
            temperature_file_path_top_3 = (
                result_directory
                + f"/temperature_time_series_top_3_{region_code}_{year}"
                + file_type
            )

            # Check if any of the files does not exist.
            if not os.path.exists(temperature_file_path_top_1) or not os.path.exists(
                temperature_file_path_top_3
            ):
                # Get the shape of the region of interest.
                region_shape = geospatial_utilities.get_geopandas_region(region_code)

                # Get the time zone information for the region.
                region_time_zone = general_utilities.get_time_zone(region_code)

            # Check if the file of temperature time series for the largest population density area does not exist.
            if not os.path.exists(temperature_file_path_top_1):
                # Get the temperature data for the largest population density area in the given region.
                temperature_time_series_top_1 = temperature_utilities.get_temperature_in_largest_population_density_areas(
                    year, region_shape, number_of_grid_cells=1
                )

                # Add temperature statistics to the time series.
                temperature_time_series_top_1 = (
                    temperature_utilities.add_temperature_statistics(
                        temperature_time_series_top_1, region_time_zone
                    )
                )

                # Save the temperature time series.
                time_series_utilities.save_time_series(
                    temperature_time_series_top_1,
                    temperature_file_path_top_1,
                    temperature_time_series_top_1.columns.values,
                    local_time_zone=region_time_zone,
                )

            # Check if the file of temperature time series for the 3 largest population density areas does not exist.
            if not os.path.exists(temperature_file_path_top_3):
                # Get the temperature data for the 3 largest population density areas in the given region.
                temperature_time_series_top_3 = temperature_utilities.get_temperature_in_largest_population_density_areas(
                    year, region_shape, number_of_grid_cells=3
                )

                # Save the temperature time series.
                time_series_utilities.save_time_series(
                    temperature_time_series_top_3,
                    temperature_file_path_top_3,
                    "Temperature (K)",
                    local_time_zone=region_time_zone,
                )


if __name__ == "__main__":
    run_temperature_calculation()
