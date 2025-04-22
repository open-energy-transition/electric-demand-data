# -*- coding: utf-8 -*-
"""
License: AGPL-3.0

Description:

    This script downloads weather data from the Copernicus Climate Data Store (CDS).

    It then extracts the weather data for the regions of interest and saves it into NetCDF files.

    The country or region codes of interest can be provided as a yaml file. If no file is provided, the script will use all available codes.

    The variable of the weather data can be specified as a command line argument. The default variable is 2m_temperature.

    The year of the weather data can be specified as a command line argument. The default year is 2015.
"""

import argparse
import logging
import os

import retrieval.weather
import util.general
import util.shapes


def read_command_line_arguments() -> argparse.Namespace:
    """
    Create a parser for the command line arguments and read them.

    Returns
    -------
    args : argparse.Namespace
        The command line arguments
    """

    # Create a parser for the command line arguments.
    parser = argparse.ArgumentParser(
        description="Download and process population density data from SEDAC."
    )

    # Add the command line arguments.
    parser.add_argument(
        "-f",
        "--file",
        type=str,
        help="The path to the yaml file containing the list of codes of the countries or regions of interest.",
        required=False,
    )
    parser.add_argument(
        "-v",
        "--variable",
        type=int,
        help="Variable of the weather data to download.",
        default="2m_temperature",
        required=False,
    )
    parser.add_argument(
        "-y",
        "--year",
        type=int,
        help="Year of the weather data to download.",
        required=False,
    )

    # Read the arguments from the command line.
    args = parser.parse_args()

    return args


def check_and_get_codes(
    args: argparse.Namespace,
) -> list[str]:
    """
    Check the validity of the country or region codes and return the list of codes of the countries or regions of interest.

    Parameters
    ----------
    args : argparse.Namespace
        The command line arguments

    Returns
    -------
    codes : list[str]
        The list of codes of the countries or regions of interest
    """

    # Get all the codes of the available countries and regions.
    all_codes = util.general.read_all_codes()

    if args.file is not None:
        # # If the file is provided, read the list of codes of the countries or regions of interest from the yaml file.
        codes = util.general.read_codes_from_file(args.file)

        # Check if the codes are valid.
        for code in codes:
            if code not in all_codes:
                logging.error(
                    f"Code {code} is not available in the list of available countries and regions."
                )
                codes.remove(code)

        # Check if there are any codes left.
        if len(codes) == 0:
            raise ValueError(
                f"None of the codes in the file are available in the list of available countries and regions. Please choose from the following codes: {all_codes}."
            )
    else:
        # If the file is not provided, use all the available codes.
        codes = all_codes

    return codes


def run_data_retrieval(args: argparse.Namespace) -> None:
    """
    Download and process the weather data from the Copernicus Climate Data Store (CDS).

    Parameters
    ----------
    args : argparse.Namespace
        The command line arguments
    """

    # Get the directory to store the population density data.
    result_directory = util.general.read_folders_structure()["weather_folder"]
    os.makedirs(result_directory, exist_ok=True)

    # Read the codes of the countries or regions of interest.
    codes = check_and_get_codes(args)

    # Define the years of the data retrieval.
    start_year = 2015
    end_year = 2015
    years = range(start_year, end_year + 1)

    # Loop over the years.
    for year in years:
        logging.info(f"Year {year}.")

        # Loop over the regions of interest.
        for code in codes:
            logging.info(f"Retrieving {args.variable} data for {code}.")

            # Define the full file path of the ERA5 data.
            era5_data_file_path = os.path.join(
                result_directory, f"{args.variable}_{code}_{year}.nc"
            )

            # Check if the file does not exist.
            if not os.path.exists(era5_data_file_path):
                # Get the region of interest.
                region_shape = util.shapes.get_region_shape(code)

                # Get the lateral bounds of the region of interest.
                region_bounds = util.shapes.get_region_bounds(
                    region_shape
                )  # West, South, East, North

                # Get the time zone of the region.
                region_time_zone = util.general.get_time_zone(code)

                # Download the ERA5 data from the Copernicus Climate Data Store (CDS).
                retrieval.weather.download_ERA5_data_from_Copernicus(
                    year,
                    args.variable,
                    era5_data_file_path,
                    region_bounds=region_bounds,
                    local_time_zone=region_time_zone,
                )

                logging.info(
                    f"{args.variable} data for {code} retrieved and saved successfully."
                )


def main() -> None:
    # Read the command line arguments.
    args = read_command_line_arguments()

    # Set up the logging configuration.
    log_file_name = "weather_data.log"
    log_files_directory = util.general.read_folders_structure()["log_files_folder"]
    os.makedirs(log_files_directory, exist_ok=True)
    logging.basicConfig(
        filename=os.path.join(log_files_directory, log_file_name),
        level=logging.INFO,
        filemode="w",
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    # Run the data retrieval.
    run_data_retrieval(args)


if __name__ == "__main__":
    main()
