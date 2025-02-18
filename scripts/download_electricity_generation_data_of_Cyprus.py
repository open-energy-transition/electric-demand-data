import logging
import os

import util.cyprus_utilities as cyprus_utilities
import util.general_utilities as general_utilities
import util.time_series_utilities as time_series_utilities


def run_electricity_generation_data_retrieval() -> None:
    """
    Run the electricity generation data retrieval from the website of the Transmission System Operator of Cyprus.
    """

    # Set up the logging configuration.
    log_files_directory = general_utilities.read_folders_structure()["log_files_folder"]
    os.makedirs(log_files_directory, exist_ok=True)
    log_file_name = "electricity_generation_data_of_Cyprus.log"
    logging.basicConfig(
        filename=log_files_directory + "/" + log_file_name,
        level=logging.INFO,
        filemode="w",
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    # Create a directory to store the electricity generation time series.
    result_directory = general_utilities.read_folders_structure()[
        "electricity_generation_folder"
    ]
    os.makedirs(result_directory, exist_ok=True)

    # Define the target file type.
    file_type = ".parquet"

    # Define the years of the data retrieval.
    start_year = 2015
    end_year = 2015
    years = range(start_year, end_year + 1)

    # Loop over the years.
    for year in years:
        logging.info(f"Year {year}.")

        # Define the file path of the electricity generation time series.
        electricity_generation_file_path = (
            result_directory + f"/electricity_generation_CY_{year}" + file_type
        )

        # Check if the file does not exist.
        if not os.path.exists(electricity_generation_file_path):
            # Retrieve the electricity generation time series.
            electricity_generation_time_series = (
                cyprus_utilities.download_electricity_generation_from_tsoc(year)
            )

            # Get the time zone of the country.
            country_time_zone = general_utilities.get_time_zone("CY")

            # Harmonize the electricity generation time series.
            electricity_generation_time_series = (
                time_series_utilities.harmonize_time_series(
                    electricity_generation_time_series,
                    country_time_zone,
                    interpolate_missing_values=False,
                )
            )

            # Save the electricity generation time series to a parquet file.
            time_series_utilities.save_time_series(
                electricity_generation_time_series,
                electricity_generation_file_path,
                "Electricity generation [MW]",
            )


if __name__ == "__main__":
    run_electricity_generation_data_retrieval()
