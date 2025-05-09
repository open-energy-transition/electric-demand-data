import os

import pycountry
import pytz
import yaml
from countryinfo import CountryInfo
from timezonefinder import TimezoneFinder


def read_folders_structure() -> dict[str, str]:
    """
    Read the folders structure from the yaml file.

    Returns
    -------
    folders_structure : dict of str
        The folders structure
    """

    # Get the absolute path to the root folder.
    root_folder = os.path.normpath(
        os.path.join(os.path.abspath(os.path.dirname(__file__)), "..")
    )

    # Define the default path to the yaml file.
    folders_structure_file_path = os.path.join(root_folder, "directories.yaml")

    # Read the folders structure from the file.
    with open(folders_structure_file_path, "r") as file:
        folders_structure = yaml.safe_load(file)

    # Add the root folder to the folders structure.
    folders_structure["root_folder"] = root_folder

    # Iterate over the folders structure, concatenate the paths if multiple folders are defined, and normalize the paths.
    for key, value in folders_structure.items():
        # Add the root folder to the path but skip the root folder key.
        if key != "root_folder":
            if isinstance(value, list):
                # If the value is a list, unpack the list.
                folders_structure[key] = os.path.join(root_folder, *value)
            else:
                folders_structure[key] = os.path.join(root_folder, value)

    return folders_structure


def read_codes_from_file(file_path: str) -> list[str]:
    """
    Read the codes of the countries or regions from a file.

    Parameters
    ----------
    file_path : str
        The path to the file containing the entries

    Returns
    -------
    codes : list of str
        The ISO Alpha-2 codes of the countries or the combination of the ISO Alpha-2 codes and the region codes
    """

    # Read the countries from the file.
    with open(file_path, "r") as file:
        data = yaml.safe_load(file)

    # Extract the entries.
    items = data["items"]

    # Extract codes.
    codes = [
        item["country_code"] + "_" + item["region_code"]
        if "region_code" in item
        else item["country_code"]
        for item in items
    ]

    return codes


def read_all_codes() -> list[str]:
    """
    Read the codes of all countries and regions.

    Returns
    -------
    codes : list of str
        The ISO Alpha-2 codes of the countries or the combination of the ISO Alpha-2 codes and the region codes
    """

    # Get the path to retrieval scripts folder.
    retrieval_scripts_directory = read_folders_structure()["retrieval_scripts_folder"]

    # Get the path of all yaml files in the retrieval scripts folder.
    yaml_files = [
        file
        for file in os.listdir(retrieval_scripts_directory)
        if file.endswith(".yaml")
    ]

    # Read the codes from all yaml files.
    codes = []
    for yaml_file in yaml_files:
        file_path = os.path.join(retrieval_scripts_directory, yaml_file)
        codes += read_codes_from_file(file_path)

    # Remove duplicates.
    codes = list(set(codes))

    return codes


def get_us_region_time_zone(region_code: str) -> pytz.timezone:
    """
    Get the time zone of a US region.

    Parameters
    ----------
    region_code : str
        The code of the US region

    Returns
    -------
    time_zone : pytz.timezone
        The time zone of the US region
    """

    # Define the time zones of the US regions.
    time_zones_mapping = {
        "US_CAL": "America/Los_Angeles",
        "US_CAR": "America/New_York",
        "US_CENT": "America/Chicago",
        "US_FLA": "America/New_York",
        "US_MIDA": "America/New_York",
        "US_MIDW": "America/Chicago",
        "US_NE": "America/New_York",
        "US_NY": "America/New_York",
        "US_NW": "America/Los_Angeles",
        "US_SE": "America/New_York",
        "US_SW": "America/Phoenix",
        "US_TEN": "America/Chicago",
        "US_TEX": "America/Chicago",
    }

    return pytz.timezone(time_zones_mapping[region_code])


def get_ca_region_time_zone(region_code: str) -> pytz.timezone:
    """
    Get the time zone of a Canadian region.

    Parameters
    ----------
    region_code : str
        The code of the Canadian region

    Returns
    -------
    time_zone : pytz.timezone
        The time zone of the Canadian region
    """

    # Define the time zones of the Canadian regions.
    time_zones_mapping = {
        "CA_AB": "America/Edmonton",
        "CA_BC": "America/Vancouver",
        "CA_MB": "America/Winnipeg",
        "CA_NB": "America/Moncton",
        "CA_NL": "America/St_Johns",
        "CA_NS": "America/Halifax",
        "CA_NT": "America/Yellowknife",
        "CA_NU": "America/Iqaluit",
        "CA_ON": "America/Toronto",
        "CA_PE": "America/Halifax",
        "CA_QC": "America/Montreal",
        "CA_SK": "America/Regina",
        "CA_YT": "America/Whitehorse",
    }

    return pytz.timezone(time_zones_mapping[region_code])


def get_time_zone(code: str) -> pytz.timezone:
    """
    Get the time zone of a country.

    Parameters
    ----------
    code : str
        The code of the region (ISO Alpha-2 code or a combination of ISO Alpha-2 code and region code)

    Returns
    -------
    time_zone : pytz.timezone
        The time zone of the region
    """

    if "_" not in code:
        # The code is the ISO Alpha-2 code of the country.
        iso_alpha_2_code = code

        # Get the list of time zones for the country.
        time_zones = pytz.country_timezones[iso_alpha_2_code]

        # If there are multiple time zones, find the time zone based on the capital city.
        if len(time_zones) > 1:
            # Get the country name.
            country = pycountry.countries.get(alpha_2=iso_alpha_2_code)

            # Get the capital city coordinates.
            location = CountryInfo(country.name).capital_latlng()

            # Find time zone based on capital city coordinates.
            tf = TimezoneFinder()
            time_zone_name = tf.timezone_at(lat=location[0], lng=location[1])
            time_zone = pytz.timezone(time_zone_name)
        else:
            # Get the time zone of the country.
            time_zone = time_zones[0]

    else:
        # Split the country code into the ISO Alpha-2 code and the region code.
        iso_alpha_2_code, region_code = code.split("_")

        if iso_alpha_2_code == "US":
            time_zone = get_us_region_time_zone(iso_alpha_2_code + "_" + region_code)
        elif iso_alpha_2_code == "CA":
            time_zone = get_ca_region_time_zone(iso_alpha_2_code + "_" + region_code)
        else:
            raise ValueError(f"The regions in {iso_alpha_2_code} are not supported.")

    return time_zone
