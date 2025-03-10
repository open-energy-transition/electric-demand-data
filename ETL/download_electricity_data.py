import argparse
import importlib
import logging
import os

import util.general as general_utilities
import util.time_series as time_series_utilities

# Define all the codes available on each platform.
codes_on_platform = {
    "ENTSO-E": [
        "AL",  # Albania
        "AT",  # Austria
        "BE",  # Belgium
        "BA",  # Bosnia and Herzegovina
        "BG",  # Bulgaria
        "CH",  # Switzerland
        "CY",  # Cyprus
        "CZ",  # Czech Republic
        "DE",  # Germany
        "DK",  # Denmark
        "EE",  # Estonia
        "ES",  # Spain
        "FI",  # Finland
        "FR",  # France
        "GB",  # United Kingdom
        "GR",  # Greece
        "HR",  # Croatia
        "HU",  # Hungary
        "IE",  # Ireland
        "IT",  # Italy
        "LT",  # Lithuania
        "LU",  # Luxembourg
        "LV",  # Latvia
        "MD",  # Moldova
        "ME",  # Montenegro
        "MK",  # North Macedonia
        "NL",  # Netherlands
        "NO",  # Norway
        "PL",  # Poland
        "PT",  # Portugal
        "RO",  # Romania
        "RS",  # Serbia
        "SE",  # Sweden
        "SI",  # Slovenia
        "SK",  # Slovakia
        "UA",  # Ukraine
        "XK",  # Kosovo
    ],
    "TSOC": ["CY"],  # Cyprus
    "NESO": ["GB_GB"],  # Great Britain
    "CCEI": [
        "CA_AB",  # Alberta
        "CA_BC",  # British Columbia
        "CA_NB",  # New Brunswick
        "CA_NL",  # Newfoundland and Labrador
        "CA_NS",  # Nova Scotia
        "CA_ON",  # Ontario
        "CA_PE",  # Prince Edward Island
        "CA_QC",  # Quebec
        # "CA_SK",  # Saskatchewan (data is at daily resolution)
        "CA_YT",  # Yukon
    ],
}


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


def check_and_get_codes(args: argparse.Namespace) -> list[str] | None:
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

    if args.code is not None:
        # Check if the code is in the list of countries or regions available on the platform.
        if args.code not in codes_on_platform[args.data_source]:
            logging.error(
                f"Code {args.code} is not available on the {args.data_source} platform."
            )
            return None
        else:
            codes = [args.code]

    elif args.file is not None:
        # Read the codes from the file.
        codes = general_utilities.read_codes_from_file(args.file)

        # Check if the codes are in the list of countries or regions available on the platform.
        for code in codes:
            if code not in codes_on_platform[args.data_source]:
                logging.error(
                    f"Code {code} is not available on the {args.data_source} platform."
                )
                codes.remove(code)

        # Check if there are any codes left.
        if len(codes) == 0:
            logging.error("No valid codes have been found.")
            return None
    else:
        # Run the data retrieval for all the countries or regions available on the platform.
        codes = codes_on_platform[args.data_source]

    return codes


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
    codes = check_and_get_codes(args)

    if codes is None:
        return None

    else:
        logging.info(
            f"Retrieving electricity data from the {args.data_source} website."
        )

        # Import the module for the data source.
        retrieval_module = importlib.import_module(f"retrieval.{args.data_source}")

        if len(codes) == 1:
            logging.info(f"Retrieving data for {codes[0]}.")

            if len(codes_on_platform[args.data_source]) == 1:
                # If there is only one code on the platform, there is no need to specify the code.
                electricity_demand_time_series = (
                    retrieval_module.download_and_extract_data()
                )
            else:
                # If there are multiple codes on the platform, the code needs to be specified.
                electricity_demand_time_series = (
                    retrieval_module.download_and_extract_data(codes[0])
                )

            # Save the electricity demand time series.
            time_series_utilities.simple_save(
                electricity_demand_time_series,
                "Load (MW)",
                result_directory,
                codes[0] + "_" + args.data_source,
            )

        else:
            # Loop over the codes.
            for code in codes:
                logging.info(f"Retrieving data for {code}.")

                # Retrieve the electricity demand time series.
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
        "electricity_load_folder"
    ]
    os.makedirs(result_directory, exist_ok=True)

    # Run the data retrieval.
    run_data_retrieval(args, result_directory)

    return None


if __name__ == "__main__":
    main()
