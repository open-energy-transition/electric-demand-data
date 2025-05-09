# -*- coding: utf-8 -*-
"""
License: AGPL-3.0

Description:

    This script downloads electricity demand data from various data sources.

    Users can specify a data source and optionally provide a country or region code to retrieve specific data.

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
import util.general
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
        "You can specify the country or region code or provide a file containing the list of codes. "
        "If no code or file is provided, the data retrieval will be run for all the countries or regions available on the data source platform."
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
        help='The ISO Alpha-2 code (example: "FR") or a combination of ISO Alpha-2 code and region code (example: "US_CAL")',
        required=False,
    )
    parser.add_argument(
        "-f",
        "--file",
        type=str,
        help="The path to the yaml file containing the list of codes",
        required=False,
    )

    # Read the arguments from the command line.
    args = parser.parse_args()

    return args


def check_and_get_codes(
    args: argparse.Namespace,
) -> tuple[list[str], bool]:
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
    one_code_on_platform : bool
        A flag to check if there is only one code on the platform
    """

    # Get the directory of the retrieval scripts.
    retrieval_scripts_directory = util.general.read_folders_structure()[
        "retrieval_scripts_folder"
    ]

    # Define the yaml file containing the list of codes.
    yaml_file_path = os.path.join(
        retrieval_scripts_directory, args.data_source.lower() + ".yaml"
    )

    # Get the list of codes available on the platform.
    codes_on_platform = util.general.read_codes_from_file(yaml_file_path)

    # Define a flag to check if there is only one code on the platform.
    one_code_on_platform = len(codes_on_platform) == 1

    if args.code is not None:
        # Check if the code is in the list of countries or regions available on the platform.
        if args.code not in codes_on_platform:
            raise ValueError(
                f"Code {args.code} is not available on the {args.data_source} platform. Please choose one of the following: {', '.join(codes_on_platform)}"
            )
        else:
            codes = [args.code]

    elif args.file is not None:
        # Read the codes from the file.
        codes = util.general.read_codes_from_file(args.file)

        # Check if the codes are in the list of countries or regions available on the platform.
        for code in codes:
            if code not in codes_on_platform:
                logging.error(
                    f"Code {code} is not available on the {args.data_source} platform."
                )
                codes.remove(code)

        # Check if there are any codes left.
        if len(codes) == 0:
            raise ValueError(
                f"None of the codes in the file are available on the {args.data_source} platform. Please choose from the following: {', '.join(codes_on_platform)}"
            )
    else:
        # Run the data retrieval for all the countries or regions available on the platform.
        codes = codes_on_platform

    return codes, one_code_on_platform


def retrieve_data(data_source: str, code: str | None) -> pandas.Series:
    """
    Retrieve the electricity demand data from the specified data source and code.

    Parameters
    ----------
    data_source : str
        The data source
    code : str, optional
        The code of the country or region

    Returns
    -------
    electricity_demand_time_series : pandas.Series
        The electricity demand time series in MW
    """

    # Get the list of requests to retrieve the electricity demand time series.
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


def run_data_retrieval(args: argparse.Namespace, result_directory: str) -> None:
    """
    Run the electricity demand data retrieval for the countries or regions of interest.

    Parameters
    ----------
    args : argparse.Namespace
        The command line arguments
    result_directory : str
        The directory to store the electricity demand time series
    """

    # Get the list of codes of the countries or regions of interest.
    codes, one_code_on_platform = check_and_get_codes(args)

    logging.info(f"Retrieving electricity data from the {args.data_source} website.")

    # Loop over the codes.
    for code in codes:
        logging.info(f"Retrieving data for {code}.")

        # Retrieve the electricity demand time series.
        electricity_demand_time_series = retrieve_data(
            args.data_source, None if one_code_on_platform else code
        )

        # Save the electricity demand time series.
        util.time_series.simple_save(
            electricity_demand_time_series,
            "Load (MW)",
            result_directory,
            code + "_" + args.data_source,
        )

    logging.info(
        f"Electricity data from the {args.data_source} website has been successfully retrieved and saved."
    )


def main() -> None:
    # Read the command line arguments.
    args = read_command_line_arguments()

    # Set up the logging configuration.
    log_file_name = f"electricity_data_from_{args.data_source}.log"
    log_files_directory = util.general.read_folders_structure()["log_files_folder"]
    os.makedirs(log_files_directory, exist_ok=True)
    logging.basicConfig(
        filename=os.path.join(log_files_directory, log_file_name),
        level=logging.INFO,
        filemode="w",
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    # Create a directory to store the electricity demand time series.
    result_directory = util.general.read_folders_structure()[
        "electricity_demand_folder"
    ]
    os.makedirs(result_directory, exist_ok=True)

    # Run the data retrieval.
    run_data_retrieval(args, result_directory)


if __name__ == "__main__":
    main()
