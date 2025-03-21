import argparse
import logging
import os

import pandas as pd
import retrieval.aeso
import retrieval.bchydro
import retrieval.ccei
import retrieval.eia
import retrieval.entsoe
import retrieval.hydroquebec
import retrieval.ieso
import retrieval.nbpower
import retrieval.neso
import retrieval.tsoc
import util.general as general_utilities
import util.time_series as time_series_utilities

retrieval_module = {
    "AESO": retrieval.aeso,
    "BCHYDRO": retrieval.bchydro,
    "CCEI": retrieval.ccei,
    "EIA": retrieval.eia,
    "ENTSOE": retrieval.entsoe,
    "HYDROQUEBEC": retrieval.hydroquebec,
    "IESO": retrieval.ieso,
    "NBPOWER": retrieval.nbpower,
    "NESO": retrieval.neso,
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
        help='The ISO Alpha-2 code (example: "US") or a combination of ISO Alpha-2 code and region code (example: "US_CAL")',
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


def check_and_format_data_source(data_source: str) -> str:
    """
    Check if the data source is valid and format it.

    Parameters
    ----------
    data_source : str
        The data source

    Returns
    -------
    data_source : str
        The formatted data source
    """

    data_source = data_source.upper()

    if data_source not in retrieval_module.keys():
        raise ValueError(
            f"Data source {data_source} is not valid. Please choose one of the following: {', '.join(retrieval_module.keys())}."
        )

    return data_source


def check_and_get_codes(
    args: argparse.Namespace,
) -> tuple[list[str] | None, bool | None]:
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

    # Get the list of codes available on the platform.
    codes_on_platform = general_utilities.read_codes_from_file(
        "retrieval/" + args.data_source.lower() + ".yaml"
    )

    # Define a flag to check if there is only one code on the platform.
    one_code_on_platform = len(codes_on_platform) == 1

    if args.code is not None:
        # Check if the code is in the list of countries or regions available on the platform.
        if args.code not in codes_on_platform:
            logging.error(
                f"Code {args.code} is not available on the {args.data_source} platform."
            )
            return None, None
        else:
            codes = [args.code]

    elif args.file is not None:
        # Read the codes from the file.
        codes = general_utilities.read_codes_from_file(args.file)

        # Check if the codes are in the list of countries or regions available on the platform.
        for code in codes:
            if code not in codes_on_platform:
                logging.error(
                    f"Code {code} is not available on the {args.data_source} platform."
                )
                codes.remove(code)

        # Check if there are any codes left.
        if len(codes) == 0:
            logging.error("No valid codes have been found.")
            return None, None
    else:
        # Run the data retrieval for all the countries or regions available on the platform.
        codes = codes_on_platform

    return codes, one_code_on_platform


def retrieve_data(data_source: str, code: str | None) -> pd.Series:
    """
    Retrieve the electricity generation data from the specified data source and code.

    Parameters
    ----------
    data_source : str
        The data source
    code : str, optional
        The code of the country or region

    Returns
    -------
    electricity_generation_time_series : pandas.Series
        The electricity generation time series in MW
    """

    # Get the list of requests to retrieve the electricity demand time series.
    requests = retrieval_module[data_source].get_available_requests()

    # If there are multiple requests (request is not None), loop over the requests to retrieve the electricity demand time series of each request.
    # If there is only one code on the platform (code is None), there is no need to specify the code.
    # If there are multiple codes on the platform, the code needs to be specified.
    if requests is None:
        if code is None:
            electricity_demand_time_series = retrieval_module[
                data_source
            ].download_and_extract_data()
        else:
            electricity_demand_time_series = retrieval_module[
                data_source
            ].download_and_extract_data(code)

    else:
        if code is None:
            if isinstance(requests[0], tuple):
                electricity_demand_time_series_list = [
                    retrieval_module[data_source].download_and_extract_data_of_request(
                        *request
                    )
                    for request in requests
                ]
            else:
                electricity_demand_time_series_list = [
                    retrieval_module[data_source].download_and_extract_data_of_request(
                        request
                    )
                    for request in requests
                ]

        else:
            if isinstance(requests[0], tuple):
                electricity_demand_time_series_list = [
                    retrieval_module[data_source].download_and_extract_data_of_request(
                        *request, code
                    )
                    for request in requests
                ]
            else:
                electricity_demand_time_series_list = [
                    retrieval_module[data_source].download_and_extract_data_of_request(
                        request, code
                    )
                    for request in requests
                ]

        # Remove empty time series.
        electricity_demand_time_series_list = [
            time_series
            for time_series in electricity_demand_time_series_list
            if not time_series.empty
        ]

        # Concatenate the electricity demand time series of all periods.
        electricity_demand_time_series = pd.concat(electricity_demand_time_series_list)

    # Clean the data.
    electricity_demand_time_series = time_series_utilities.clean_data(
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

    if codes is not None:
        logging.info(
            f"Retrieving electricity data from the {args.data_source} website."
        )

        # Loop over the codes.
        for code in codes:
            logging.info(f"Retrieving data for {code}.")

            # Retrieve the electricity demand time series.
            electricity_demand_time_series = retrieve_data(
                args.data_source, None if one_code_on_platform else code
            )

            # Save the electricity demand time series.
            time_series_utilities.simple_save(
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

    # Check the validity of the data source.
    args.data_source = check_and_format_data_source(args.data_source)

    # Set up the logging configuration.
    log_file_name = f"electricity_data_from_{args.data_source}.log"
    log_files_directory = general_utilities.read_folders_structure()["log_files_folder"]
    os.makedirs(log_files_directory, exist_ok=True)
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

    # Run the data retrieval.
    run_data_retrieval(args, result_directory)


if __name__ == "__main__":
    main()
