# -*- coding: utf-8 -*-
"""
License: AGPL-3.0

Description:

    This script downloads electricity demand data from various data sources.

    Users can specify a data source and optionally provide a country or subdivision code to retrieve specific data.

    The retrieved data is cleaned and saved in a structured format for further analysis.
"""

import argparse
import logging
import os

import pandas
import retrieval.aemo_nem
import retrieval.aemo_wem
import retrieval.aeso
import retrieval.bchydro
import retrieval.cammesa
import retrieval.ccei
import retrieval.cen
import retrieval.coes
import retrieval.eia
import retrieval.entsoe
import retrieval.eskom
import retrieval.hydroquebec
import retrieval.ieso
import retrieval.nbpower
import retrieval.neso
import retrieval.nigeria
import retrieval.ons
import retrieval.sonelgaz
import retrieval.tepco
import retrieval.tsoc
import util.directories
import util.entities
import util.time_series

retrieval_module = {
    "AEMO_NEM": retrieval.aemo_nem,
    "AEMO_WEM": retrieval.aemo_wem,
    "AESO": retrieval.aeso,
    "BCHYDRO": retrieval.bchydro,
    "CAMMESA": retrieval.cammesa,
    "CCEI": retrieval.ccei,
    "CEN": retrieval.cen,
    "COES": retrieval.coes,
    "EIA": retrieval.eia,
    "ENTSOE": retrieval.entsoe,
    "ESKOM": retrieval.eskom,
    "HYDROQUEBEC": retrieval.hydroquebec,
    "IESO": retrieval.ieso,
    "NBPOWER": retrieval.nbpower,
    "NESO": retrieval.neso,
    "NIGERIA": retrieval.nigeria,
    "ONS": retrieval.ons,
    "SONELGAZ": retrieval.sonelgaz,
    "TEPCO": retrieval.tepco,
    "TSOC": retrieval.tsoc,
}


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
        description="Download electricity demand data from the specified data source. "
        "You can specify the country or subdivision code or provide a file containing the list of codes. "
        "If no code or file is provided, the data retrieval will be run for all the countries and subdivisions available on the data source website."
    )
    parser.add_argument(
        "data_source",
        type=str,
        choices=retrieval_module.keys(),
        help='The acronym of the data source as defined in the retrieval modules (example: "ENTSOE")',
    )
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
        help="The path to the yaml file containing the list of codes of the countries and subdivisions of interest.",
        required=False,
    )

    # Read the arguments from the command line.
    args = parser.parse_args()

    return args


def retrieve_data(data_source: str, code: str | None) -> pandas.Series:
    """
    Retrieve the electricity demand data from the specified data source and code.

    Parameters
    ----------
    data_source : str
        The data source
    code : str, optional
        The code of the country or subdivision

    Returns
    -------
    electricity_demand_time_series : pandas.Series
        The electricity demand time series in MW
    """

    # Get the list of requests to retrieve the electricity demand time series.
    if code is None:
        # If there is only one code on the platform (code is None), there is no need to specify the code.
        requests = retrieval_module[data_source].get_available_requests()
    else:
        # If there are multiple codes on the platform (code is not None), the code needs to be specified.
        requests = retrieval_module[data_source].get_available_requests(code)

    if requests is None:
        # If there are no requests (requests is None), it means that the electricity demand time series can be retrieved all at once.
        # Get the retrieval function to download and extract the data.
        retrieval_function = retrieval_module[data_source].download_and_extract_data

        if code is None:
            # If there is only one code on the platform (code is None), there is no need to specify the code.
            electricity_demand_time_series = retrieval_function()
        else:
            # If there are multiple codes on the platform (code is not None), the code needs to be specified.
            electricity_demand_time_series = retrieval_function(code)

    else:
        # If there are multiple requests (request is not None), loop over the requests to retrieve the electricity demand time series of each request.
        # Get the retrieval function to download and extract the data.
        retrieval_function = retrieval_module[
            data_source
        ].download_and_extract_data_for_request

        if code is None:
            # If there is only one code on the platform (code is None), there is no need to specify the code.
            # Loop over the requests to retrieve the electricity demand time series of each request.
            electricity_demand_time_series_list = [
                retrieval_function(*request)
                if isinstance(request, tuple)
                else retrieval_function(request)
                for request in requests
            ]
        else:
            # If there are multiple codes on the platform (code is not None), the code needs to be specified.
            # Loop over the requests to retrieve the electricity demand time series of each request.
            electricity_demand_time_series_list = [
                retrieval_function(*request, code)
                if isinstance(request, tuple)
                else retrieval_function(request, code)
                for request in requests
            ]

        # Remove empty time series.
        electricity_demand_time_series_list = [
            time_series
            for time_series in electricity_demand_time_series_list
            if not time_series.empty
        ]

        # Concatenate the electricity demand time series of all periods.
        electricity_demand_time_series = pandas.concat(
            electricity_demand_time_series_list
        )

    # Clean the data.
    electricity_demand_time_series = util.time_series.clean_data(
        electricity_demand_time_series
    )

    return electricity_demand_time_series


def save_data(
    electricity_demand_time_series: pandas.Series,
    data_source: str,
    code: str,
) -> None:
    """
    Save the electricity demand time series to a file.

    Parameters
    ----------
    electricity_demand_time_series : pandas.Series
        The electricity demand time series in MW
    data_source : str
        The data source
    code : str, optional
        The code of the country or subdivision
    """

    # Get the directory to store the electricity demand time series.
    result_directory = util.directories.read_folders_structure()[
        "electricity_demand_folder"
    ]
    os.makedirs(result_directory, exist_ok=True)

    # Get the date of retrieval.
    date_of_retrieval = pandas.Timestamp.today().strftime("%Y-%m-%d")

    # Define the identifier of the file to be saved.
    identifier = code + "_" + data_source

    # Define the path to the file to be saved without the extension.
    file_path = os.path.join(result_directory, identifier + "_" + date_of_retrieval)

    # Save the time series to both parquet and csv files.
    electricity_demand_time_series.to_frame().to_parquet(file_path + ".parquet")
    electricity_demand_time_series.to_csv(file_path + ".csv")


def run_data_retrieval(args: argparse.Namespace) -> None:
    """
    Run the electricity demand data retrieval for the countries and subdivisions of interest.

    Parameters
    ----------
    args : argparse.Namespace
        The command line arguments
    result_directory : str
        The directory to store the electricity demand time series
    """

    # Get the list of codes of the countries and subdivisions of interest.
    codes = util.entities.check_and_get_codes(args)

    # Check if there is only one code on the platform.
    one_code_in_data_source = len(util.entities.read_codes(args.data_source)) == 1

    logging.info(f"Retrieving electricity data from the {args.data_source} website.")

    # Loop over the codes.
    for code in codes:
        logging.info(f"Retrieving data for {code}.")

        # Retrieve the electricity demand time series.
        electricity_demand_time_series = retrieve_data(
            args.data_source, None if one_code_in_data_source else code
        )

        # Save the electricity demand time series to a file and upload it to GCS.
        save_data(electricity_demand_time_series, args.data_source, code)

    logging.info(
        f"Electricity data from the {args.data_source} website has been successfully retrieved and saved."
    )


if __name__ == "__main__":
    # Read the command line arguments.
    args = read_command_line_arguments()

    # Set up the logging configuration.
    log_file_name = f"electricity_data_from_{args.data_source}.log"
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
