import logging
import os

import util.cds_utilities as cds_utilities
import util.general_utilities as general_utilities
import util.geospatial_utilities as geospatial_utilities


def run_weather_data_retrieval() -> None:
    """
    Run the weather data retrieval from the Copernicus Climate Data Store (CDS).
    """

    # Set up the logging configuration.
    log_files_directory = general_utilities.read_folders_structure()["log_files_folder"]
    os.makedirs(log_files_directory, exist_ok=True)
    log_file_name = "weather_data_from_Copernicus.log"
    logging.basicConfig(
        filename=log_files_directory + "/" + log_file_name,
        level=logging.INFO,
        filemode="w",
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    # Create a directory to store the weather data.
    result_directory = general_utilities.read_folders_structure()["weather_folder"]
    os.makedirs(result_directory, exist_ok=True)

    # Read the codes of the regions of interest.
    settings_directory = general_utilities.read_folders_structure()["settings_folder"]
    # region_codes = general_utilities.read_countries_from_file(settings_directory+"/gegis__all_countries.txt")
    region_codes = general_utilities.read_us_regions_from_file(
        settings_directory + "/us_eia_regions.txt"
    )

    # Define the ERA5 variables to download.
    era5_variables = ["2m_temperature"]

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

            # Loop over the ERA5 variables.
            for era5_variable in era5_variables:
                logging.info(f"Retrieving {era5_variable} data...")

                # Define the full file path of the ERA5 data.
                era5_data_file_path = (
                    result_directory + f"/{era5_variable}_{region_code}_{year}.nc"
                )

                # Check if the file does not exist.
                if not os.path.exists(era5_data_file_path):
                    # Get the region of interest.
                    region_shape = geospatial_utilities.get_geopandas_region(
                        region_code
                    )

                    # Get the lateral bounds of the region of interest.
                    region_bounds = geospatial_utilities.get_region_bounds(
                        region_shape
                    )  # West, South, East, North

                    # Get the time zone of the region.
                    region_time_zone = general_utilities.get_time_zone(region_code)

                    # Download the ERA5 data from the Copernicus Climate Data Store (CDS).
                    cds_utilities.download_ERA5_data_from_Copernicus(
                        year,
                        era5_variable,
                        era5_data_file_path,
                        region_bounds=region_bounds,
                        local_time_zone=region_time_zone,
                    )

                    logging.info(f"Downloaded {era5_variable} data.")


if __name__ == "__main__":
    run_weather_data_retrieval()
