# -*- coding: utf-8 -*-
"""
License: AGPL-3.0.

Description:

    This script downloads GPD data from a Zenodo repository. It then
    extracts the GDP data for the countries and subdivisions of interest
    at a 0.25-degree resolution, and saves it into NetCDF files. The
    year of the GPD data can be specified as a command line argument.
    The default year is 2020.

    Source: https://zenodo.org/records/7898409
    Source: https://doi.org/10.1038/s41597-022-01300-x

"""

import argparse
import io
import logging
import os

import py7zr
import requests
import utils.directories
import utils.entities
import utils.figures
import utils.geospatial
import utils.shapes
import xarray


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
            "Download and process GPD data from a Zenodo repository. You can "
            "specify the country or subdivision code, provide a file "
            "containing the list of codes, or use all available codes. The "
            "year of the GDP data can also be specified."
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
        help="Year of the GDP data to be downloaded",
        required=False,
    )

    # Read the arguments from the command line.
    args = parser.parse_args()

    return args


def run_data_retrieval(args: argparse.Namespace) -> None:
    """
    Download and extract GDP data.

    This function downloads GDP data from a Zenodo repository, extracts
    the GDP data for the countries and subdivisions of interest at a
    0.25-degree resolution, and saves it into NetCDF files.

    Parameters
    ----------
    args : argparse.Namespace
        The command line arguments.
    """
    # Get the directory to store the population density data.
    result_directory = utils.directories.read_folders_structure()["gdp_folder"]
    os.makedirs(result_directory, exist_ok=True)

    if args.year is not None:
        years = [args.year]
    else:
        years = list(range(2000, 2021))

    # Get the list of codes of the countries and subdivisions of
    # interest.
    codes = utils.entities.check_and_get_codes(
        code=args.code, file_path=args.file
    )

    # Loop over the years.
    for year in years:
        logging.info(f"Downloading GDP data for the year {year}.")

        # Fetch the GDP data from Zenodo.
        response = requests.get(
            "https://zenodo.org/records/7898409/files/"
            "GDP_025d%20(2000-2100).7z?download=1"
        )

        # Extract the archive from the response.
        with py7zr.SevenZipFile(io.BytesIO(response.content), mode="r") as archive:

            with tempfile.TemporaryDirectory() as temp_dir:
                # Extract only the specific file we need to a temporary directory
                archive.extract(path=temp_dir, targets=[f"025d/GDP{year}.tif"])
                
                # Open the GDP data for the specified year
                tif_path = os.path.join(temp_dir, f"025d/GDP{year}.tif")
                global_gdp = xarray.open_dataarray(
                    tif_path,
                    engine="rasterio",
                )


        # Harmonize the GDP data.
        global_gdp = utils.geospatial.harmonize_coords(global_gdp)

        # Clean the dataset.
        global_gdp = utils.geospatial.clean_raster(global_gdp, "gdp")

        # Loop over the countries and subdivisions of interest.
        for code in codes:
            # Define the file path of the population density data for
            # the country or subdivision.
            file_path = os.path.join(
                result_directory, f"{code}_0.25_deg_{year}.nc"
            )

            if not os.path.exists(file_path):
                logging.info(f"Extracting GDP data of {code}.")

                # Get the shape of the country or subdivision.
                entity_shape = utils.shapes.get_entity_shape(
                    code, make_plot=False
                )

                # Get the lateral bounds of the country or subdivision
                # of interest.
                entity_bounds = utils.shapes.get_entity_bounds(
                    entity_shape
                )  # West, South, East, North

                # Select the GDP data for the country or subdivision of
                # interest.
                gdp = global_gdp.sel(
                    x=slice(entity_bounds[0], entity_bounds[2]),
                    y=slice(entity_bounds[1], entity_bounds[3]),
                )

                # Save the GDP data.
                gdp.to_netcdf(file_path)

                make_plot = False
                if make_plot:
                    # Make a plot of the GDP data.
                    utils.figures.simple_plot(gdp, f"gdp_{code}_{year}")

                logging.info(
                    f"GDP data for {code} has been successfully extracted and "
                    "saved."
                )

            else:
                logging.info(
                    f"GDP data for {code} already exists. Skipping extraction."
                )


if __name__ == "__main__":
    # Read the command line arguments.
    args = read_command_line_arguments()

    # Set up the logging configuration.
    log_file_name = "gdp_data.log"
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
