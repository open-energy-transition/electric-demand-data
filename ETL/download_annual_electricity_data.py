# -*- coding: utf-8 -*-
"""
License: AGPL-3.0.

Description:

    This script downloads annual electricity demand data from Ember. It
    then extracts the electricity data for the countries and
    subdivisions of interest and saves it into CSV and Parquet files.
    The year of the electricity data can be specified as a command line
    argument. If no year is provided, the script will use all the years
    of available electricity demand data.

    Source: https://ember-energy.org/data/yearly-electricity-data/
"""

import argparse
import logging
import os
from datetime import datetime

import pandas
import utils.directories
import utils.entities


def read_command_line_arguments() -> argparse.Namespace:
    """
    Create a parser for the command line arguments and read them.

    Returns
    -------
    args : argparse.Namespace
        The command line arguments.
    """
    # Create a parser for the command line arguments.
    parser = argparse.ArgumentParser(
        description=(
            "Download and process annual electricity demand data from Ember. "
            "You can specify the country or subdivision code, provide a file "
            "containing the list of codes, or use all available codes. The "
            "year of the electricity data can also be specified."
        )
    )

    # Add the command line arguments.
    parser.add_argument(
        "-c",
        "--code",
        type=str,
        help=(
            'The ISO Alpha-2 code (example: "FR") or a combination of ISO '
            'Alpha-2 code and subdivision code (example: "US_CAL")'
        ),
        required=False,
    )
    parser.add_argument(
        "-f",
        "--file",
        type=str,
        help=(
            "The path to the yaml file containing the list of codes of the "
            "countries and subdivisions of interest"
        ),
        required=False,
    )
    parser.add_argument(
        "-y",
        "--year",
        type=int,
        choices=list(range(2000, 2021)),
        help="Year of the electricity data to be downloaded",
        required=False,
    )

    # Read the arguments from the command line.
    args = parser.parse_args()

    return args


def run_data_retrieval(args: argparse.Namespace) -> None:
    """
    Download and extract GDP data.

    This function downloads the annual electricity demand data from
    Ember and extracts the electricity data for the countries and
    subdivisions of interest. The data is saved into CSV and Parquet
    files.

    Parameters
    ----------
    args : argparse.Namespace
        The command line arguments.
    """
    # Get the directory to store the population density data.
    result_directory = utils.directories.read_folders_structure()[
        "annual_electricity_demand_folder"
    ]
    os.makedirs(result_directory, exist_ok=True)

    # Get the list of codes of the countries and subdivisions.
    codes = utils.entities.check_and_get_codes(
        code=args.code, file_path=args.file
    )

    logging.info("Downloading annual electricity data.")

    # Read the CSV file containing the electricity data
    global_electricity_data = pandas.read_csv(
        "https://storage.googleapis.com/emb-prod-bkt-publicdata/"
        "public-downloads/yearly_full_release_long_format.csv"
    )

    # Loop over the countries and subdivisions.
    for code in codes:
        # Define the file path of the population density data for the
        # country or subdivision.
        file_path = os.path.join(result_directory, f"{code}.parquet")

        if not os.path.exists(file_path):
            logging.info(f"Extracting annual electricity data of {code}.")

            if args.year is not None:
                # If the year is provided, use it.
                years = [args.year]
            else:
                # Get the years of available data for the country or
                # subdivision.
                years = utils.entities.get_available_years(code)

            # Get the ISO Alpha-3 code of the country.
            iso_alpha_3_code = utils.entities.get_iso_alpha_3_code(code)

            # Extract the electricity data for the country and years of
            # interest.
            country_electricity_data = global_electricity_data[
                (global_electricity_data["ISO 3 code"] == iso_alpha_3_code)
                & (global_electricity_data["Year"].isin(years))
            ]

            # Extract the electricity demand and demand per capita data.
            electricity_demand = pandas.Series(
                country_electricity_data[
                    country_electricity_data["Variable"] == "Demand"
                ]["Value"].values,
                index=country_electricity_data[
                    country_electricity_data["Variable"] == "Demand"
                ]["Year"],
            )
            electricity_demand_per_capita = pandas.Series(
                country_electricity_data[
                    country_electricity_data["Variable"] == "Demand per capita"
                ]["Value"].values,
                index=country_electricity_data[
                    country_electricity_data["Variable"] == "Demand per capita"
                ]["Year"],
            )

            # Get the time zone of the country.
            time_zone = utils.entities.get_time_zone(code)

            # Define a new index with hourly frequency in the local time
            # zone.
            index = pandas.date_range(
                start=f"{str(country_electricity_data['Year'].min())}-01-01",
                end=(
                    f"{str(country_electricity_data['Year'].max())}-12-31 "
                    "23:00:00"
                ),
                freq="h",
                tz=time_zone,
            )

            # Create a DataFrame with the new index.
            country_electricity_data = pandas.DataFrame(index=index)

            # Map the electricity demand and demand per capita data to
            # the new index.
            country_electricity_data["Annual electricity demand (TWh)"] = (
                country_electricity_data.index.year.map(
                    electricity_demand
                ).to_numpy()
            )
            country_electricity_data[
                "Annual electricity demand per capita (MWh)"
            ] = country_electricity_data.index.year.map(
                electricity_demand_per_capita
            ).to_numpy()

            # Convert the index to UTC and remove the time zone
            # information.
            country_electricity_data.index = (
                country_electricity_data.index.tz_convert("UTC").tz_localize(
                    None
                )
            )

            # Set the index name.
            country_electricity_data.index.name = "Time (UTC)"

            # Save the electricity demand to parquet and CSV files.
            country_electricity_data.to_parquet(file_path)
            country_electricity_data.to_csv(
                file_path.replace(".parquet", ".csv")
            )

            logging.info(
                f"Annual electricity data of {code} has been extracted and "
                "saved successfully."
            )

        else:
            logging.info(
                f"Annual electricity data of {code} already exists. Skipping "
                "download."
            )


if __name__ == "__main__":
    # Read the command line arguments.
    args = read_command_line_arguments()

    # Set up the logging configuration.
    log_file_name = (
        "gdp_data_" + datetime.now().strftime("%Y%m%d_%H%M") + ".log"
    )
    log_files_directory = utils.directories.read_folders_structure()[
        "log_files_folder"
    ]
    os.makedirs(log_files_directory, exist_ok=True)
    logging.basicConfig(
        filename=os.path.join(log_files_directory, log_file_name),
        level=logging.INFO,
        filemode="w",
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    # Run the data retrieval.
    run_data_retrieval(args)
