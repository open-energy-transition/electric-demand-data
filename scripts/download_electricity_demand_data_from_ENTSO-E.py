import logging
import os

import util.entsoe_utilities as entsoe_utilities
import util.general_utilities as general_utilities
import util.time_series_utilities as time_series_utilities


def run_electricity_demand_data_retrieval() -> None:
    """
    Run the electricity demand data retrieval from the ENTSO-E API for the countries of interest and the years of interest.
    """

    # Set up the logging configuration.
    log_files_directory = general_utilities.read_folders_structure()["log_files_folder"]
    log_file_name = "electricity_demand_data_from_ENTSO-E.log"
    logging.basicConfig(
        filename=log_files_directory + "/" + log_file_name,
        level=logging.INFO,
        filemode="w",
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    # Create a directory to store the electricity demand time series.
    result_directory = general_utilities.read_folders_structure()[
        "electricity_demand_folder"
    ]
    os.makedirs(result_directory, exist_ok=True)

    # Read the ISO Alpha-2 codes of the countries of interest.
    settings_directory = general_utilities.read_folders_structure()["settings_folder"]
    iso_alpha_2_codes = general_utilities.read_countries_from_file(
        settings_directory + "/gegis__countries_on_entsoe_platform.txt"
    )

    # Define the target file type.
    file_type = ".parquet"

    # Define the years of the data retrieval.
    start_year = 2015
    end_year = 2015
    years = range(start_year, end_year + 1)

    # Loop over the years.
    for year in years:
        logging.info(f"Year {year}.")

        # Loop over the countries.
        for iso_alpha_2_code in iso_alpha_2_codes:
            logging.info(f"Retrieving data for {iso_alpha_2_code}...")

            # Define the file path of the electricity demand time series.
            electricity_demand_file_path = (
                result_directory
                + f"/electricity_demand_{iso_alpha_2_code}_{year}"
                + file_type
            )

            # Check if the file does not exist.
            if not os.path.exists(electricity_demand_file_path):
                # Download the electricity demand time series from the ENTSO-E API.
                entsoe_electricity_demand_time_series = (
                    entsoe_utilities.download_electricity_demand_from_entsoe(
                        year, iso_alpha_2_code
                    )
                )

                if entsoe_electricity_demand_time_series is not None:
                    # Get the local time zone of the country.
                    country_time_zone = general_utilities.get_time_zone(
                        iso_alpha_2_code
                    )

                    # Harmonize the electricity demand time series.
                    entsoe_electricity_demand_time_series = (
                        time_series_utilities.harmonize_time_series(
                            entsoe_electricity_demand_time_series,
                            country_time_zone,
                            interpolate_missing_values=False,
                        )
                    )

                    # Save the electricity demand time series to a parquet file.
                    time_series_utilities.save_time_series(
                        entsoe_electricity_demand_time_series,
                        electricity_demand_file_path,
                        "Electricity demand [MW]",
                    )


if __name__ == "__main__":
    run_electricity_demand_data_retrieval()
