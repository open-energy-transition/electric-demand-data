# -*- coding: utf-8 -*-
"""
License: AGPL-3.0

Description:

    This script downloads population density data from SEDAC (Socioeconomic Data and Applications Center).

    It then extracts the population density data for the countries and subdivisions of interest, coarsens the data to a 0.25-degree resolution, and saves it into NetCDF files.

    The country and subdivision code can be specified or a list can be provided as a yaml file. If no file or code is provided, the script will use all available codes.

    The year of the population density data can be specified as a command line argument. The default year is 2020.
"""

import argparse
import logging
import os

import util.directories
import util.entities
import util.figures
import util.geospatial
import util.shapes
import xarray


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
        description="Download and process population density data from SEDAC. "
        "You can specify the country or subdivision code, provide a file containing the list of codes, "
        "or use all available codes. The year of the population density data can also be specified."
    )

    # Add the command line arguments.
    parser.add_argument(
        "-c",
        "--code",
        type=str,
        help='The ISO Alpha-2 code (example: "FR") or a combination of ISO Alpha-2 code and subdivision code (example: "US_CAL")',
        required=False,
    )
    parser.add_argument(
        "-f",
        "--file",
        type=str,
        help="The path to the yaml file containing the list of codes of the countries and subdivisions of interest",
        required=False,
    )
    parser.add_argument(
        "-y",
        "--year",
        type=int,
        choices=[2000, 2005, 2010, 2015, 2020],
        help="Year of the population density data to be downloaded",
        required=False,
    )

    # Read the arguments from the command line.
    args = parser.parse_args()

    return args


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

    if args.year is not None:
        years = [args.year]
    else:
        years = [2000, 2005, 2010, 2015, 2020]

    # Get the list of codes of the countries and subdivisions of interest.
    codes = util.entities.check_and_get_codes(code=args.code, file_path=args.file)

    # Loop over the years.
    for year in years:
        logging.info(f"Downloading population density data for the year {year}.")

        # Define the URL of the population density data.
        url = f"https://data.ghg.center/sedac-popdensity-yeargrid5yr-v4.11/gpw_v4_population_density_rev11_{year}_30_sec_{year}.tif"

        # Download the population density data.
        global_population_density = xarray.open_dataarray(url, engine="rasterio")

        # Harmonize the population density data.
        global_population_density = util.geospatial.harmonize_coords(
            global_population_density
        )

        # Clean the dataset.
        global_population_density = util.geospatial.clean_raster(
            global_population_density, "population_density"
        )

        # Loop over the countries and subdivisions of interest.
        for code in codes:
            # Define the file path of the population density data for the country or subdivision.
            file_path = os.path.join(result_directory, f"{code}_0.25_deg_{year}.nc")

            if not os.path.exists(file_path):
                logging.info(f"Extracting population density data of {code}.")

                # Get the shape of the country or subdivision.
                entity_shape = util.shapes.get_entity_shape(code, make_plot=False)

                # Get the lateral bounds of the country or subdivision of interest.
                entity_bounds = util.shapes.get_entity_bounds(
                    entity_shape
                )  # West, South, East, North

                # Select the population density data in the bounding box of the country or subdivision of interest.
                population_density = global_population_density.sel(
                    x=slice(entity_bounds[0], entity_bounds[2]),
                    y=slice(entity_bounds[1], entity_bounds[3]),
                )

                # Coarsen the population density data to the same resolution as the weather data.
                population_density = util.geospatial.coarsen(
                    population_density, entity_bounds
                )

                # Save the population density data.
                population_density.to_netcdf(file_path)

                make_plot = False
                if make_plot:
                    # Make a plot of the population density data.
                    util.figures.simple_plot(
                        population_density,
                        f"population_density_{code}_{year}",
                    )

                logging.info(
                    f"Population density data for {code} has been successfully extracted and saved."
                )

            else:
                logging.info(
                    f"Population density data for {code} already exists. Skipping extraction."
                )


if __name__ == "__main__":
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
