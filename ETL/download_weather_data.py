# -*- coding: utf-8 -*-
"""
License: AGPL-3.0

Description:

    This script downloads weather data from the Copernicus Climate Data Store (CDS).

    It then extracts the weather data for the countries and subdivisions of interest and saves it into NetCDF files.

    The country and subdivision codes of interest can be provided as a yaml file. If no file is provided, the script will use all available codes.

    The variable of the weather data can be specified as a command line argument. The default variable is 2m_temperature.

    The year of the weather data can be specified as a command line argument. The default year is 2015.
"""

import argparse
import logging
import os

import retrieval.weather
import util.directories
import util.shapes
import util.entities


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
        help="The path to the yaml file containing the list of codes of the countries and subdivisions of interest.",
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
    Check the validity of the country and subdivision codes and return the list of codes of the countries and subdivisions of interest.

    Parameters
    ----------
    args : argparse.Namespace
        The command line arguments

    Returns
    -------
    codes : list[str]
        The list of codes of the countries and subdivisions of interest
    """

    # Get all the codes of the available countries and subdivisions.
    all_codes = util.entities.read_all_codes()

    if args.file is not None:
        # # If the file is provided, read the list of codes of the countries and subdivisions of interest from the yaml file.
        codes = util.entities.read_codes(file_path=args.file)

        # Check if the codes are valid.
        for code in codes:
            if code not in all_codes:
                logging.error(
                    f"Code {code} is not available in the list of available countries and subdivisions."
                )
                codes.remove(code)

        # Check if there are any codes left.
        if len(codes) == 0:
            raise ValueError(
                f"None of the codes in the file are available in the list of available countries and subdivisions. Please choose from the following codes: {all_codes}."
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
    result_directory = util.directories.read_folders_structure()["weather_folder"]
    os.makedirs(result_directory, exist_ok=True)

    # Read the codes of the countries and subdivisions of interest.
    codes = check_and_get_codes(args)

    # Loop over the countries and subdivisions of interest.
    for code in codes:
        logging.info(f"Retrieving {args.variable} data for {code}.")

        if args.year is not None:
            # If the year is provided, use it.
            years = [args.year]
        else:
            # Read the start and end dates of the available data for the country or subdivision of interest.
            start_and_end_dates = util.entities.read_all_date_ranges()[code]

            # Get the years of the data retrieval.
            years = [year for year in range(start_and_end_dates[0].year, start_and_end_dates[1].year + 1)]

        # Loop over the years.
        for year in years:

            logging.info(f"Retrieving {args.variable} data in {year}")

            # Define the full file path of the ERA5 data.
            era5_data_file_path = os.path.join(
                result_directory, f"{args.variable}_{code}_{year}.nc"
            )

            # Check if the file does not exist.
            if not os.path.exists(era5_data_file_path):
                # Get the shape of the country or subdivision.
                entity_shape = util.shapes.get_entity_shape(code)

                # Get the lateral bounds of the country or subdivision.
                entity_bounds = util.shapes.get_entity_bounds(
                    entity_shape
                )  # West, South, East, North

                # Get the time zone of the country or subdivision.
                entity_time_zone = util.entities.get_time_zone(code)

                # Download the ERA5 data from the Copernicus Climate Data Store (CDS).
                retrieval.weather.download_ERA5_data_from_Copernicus(
                    year,
                    args.variable,
                    era5_data_file_path,
                    entity_bounds=entity_bounds,
                    local_time_zone=entity_time_zone,
                )

                logging.info(
                    f"{args.variable} data for {code} retrieved and saved successfully."
                )


def main() -> None:
    # Read the command line arguments.
    args = read_command_line_arguments()

    # Set up the logging configuration.
    log_file_name = "weather_data.log"
    log_files_directory = util.directories.read_folders_structure()["log_files_folder"]
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
