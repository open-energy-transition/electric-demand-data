# -*- coding: utf-8 -*-
"""
License: AGPL-3.0

Description:

    This script downloads population density data from SEDAC (Socioeconomic Data and Applications Center).

    It then extracts the population density data for the countries and subdivisions of interest, coarsens the data to a 0.25-degree resolution, and saves it into NetCDF files.

    The country or subdivision codes of interest can be provided as a yaml file. If no file is provided, the script will use all available codes.

    The year of the population density data can be specified as a command line argument. The default year is 2020.
"""

import argparse
import logging
import os

import retrieval.population
import util.directories
import util.geospatial
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
        "-y",
        "--year",
        type=int,
        choices=[2000, 2005, 2010, 2015, 2020],
        help="Year of the population density data to download.",
        default=2020,
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
    Download the population density data from SEDAC and extract the population density data for the countries and subdivisions of interest.

    Parameters
    ----------
    args : argparse.Namespace
        The command line arguments
    """

    # Get the directory to store the population density data.
    result_directory = util.directories.read_folders_structure()[
        "population_density_folder"
    ]
    os.makedirs(result_directory, exist_ok=True)

    # Load the population density data.
    population_density = util.geospatial.load_xarray(
        retrieval.population.get_url(args.year), engine="rasterio"
    )

    # Read the codes of the countries and subdivisions of interest.
    codes = check_and_get_codes(args)

    # Loop over the countries and subdivisions of interest.
    for code in codes:
        # Define the file path of the population density data for the country or subdivision.
        population_file_path = os.path.join(
            result_directory, f"{code}_0.25_deg_{args.year}.nc"
        )

        if not os.path.exists(population_file_path):
            logging.info(f"Extracting population density of {code}.")

            # Get the shape of the country or subdivision.
            entity_shape = util.shapes.get_entity_shape(code)

            # Extract the population density of the country or subdivision.
            retrieval.population.extract_population_density_of_entity(
                population_density, entity_shape, population_file_path
            )

            logging.info(
                f"Population density data for {code} extracted and saved successfully."
            )

        else:
            logging.info(
                f"Population density data for {code} already exists. Skipping extraction."
            )


def main() -> None:
    # Read the command line arguments.
    args = read_command_line_arguments()

    # Set up the logging configuration.
    log_file_name = "population_density_data.log"
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
