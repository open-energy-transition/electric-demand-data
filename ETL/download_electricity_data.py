import argparse
import importlib
import logging
import os

import util.general as general_utilities
import util.time_series as time_series_utilities


def read_command_line_arguments():
    """
    Create a parser for the command line arguments and read them.
    """

    # Create a parser for the command line arguments.
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "data_source",
        type=str,
        help='The acronym of the data source as defined in the retrieval modules (example: "ENTSO-E")',
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


def check_and_get_codes(args: argparse.Namespace) -> tuple[list[str] | None, bool]:
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
        "retrieval/" + args.data_source + ".yaml"
    )

    # Define a flag to check if there is only one code on the platform.
    one_code_on_platform = len(codes_on_platform) == 1

    if args.code is not None:
        # Check if the code is in the list of countries or regions available on the platform.
        if args.code not in codes_on_platform:
            logging.error(
                f"Code {args.code} is not available on the {args.data_source} platform."
            )
            codes = None
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
            codes = None
    else:
        # Run the data retrieval for all the countries or regions available on the platform.
        codes = codes_on_platform

    return codes, one_code_on_platform


def run_data_retrieval(args: argparse.Namespace, result_directory: str) -> None:
    """
    Run the electricity demand data retrieval for the countries or regions of interest.

    Parameters
    ----------
    args : argparse.Namespace
        The command line arguments
    result_directory : str
        The directory to store the electricity demand time series
    codes : str | list[str]
        The code or list of codes of the countries or regions of interest
    """

    # Get the list of codes of the countries or regions of interest.
    codes, one_code_on_platform = check_and_get_codes(args)

    if codes is None:
        return None

    else:
        logging.info(
            f"Retrieving electricity data from the {args.data_source} website."
        )

        # Import the module for the data source.
        retrieval_module = importlib.import_module(f"retrieval.{args.data_source}")

        # Loop over the codes.
        for code in codes:
            logging.info(f"Retrieving data for {code}.")

            # Retrieve the electricity demand time series.
            if one_code_on_platform:
                # If there is only one code on the platform (and only one code in the list of codes), there is no need to specify the code.
                electricity_demand_time_series = (
                    retrieval_module.download_and_extract_data()
                )
            else:
                # If there are multiple codes on the platform, the code needs to be specified.
                electricity_demand_time_series = (
                    retrieval_module.download_and_extract_data(code)
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
