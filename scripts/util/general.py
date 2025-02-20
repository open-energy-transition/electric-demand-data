import logging

import pycountry
import pytz
import yaml
from countryinfo import CountryInfo
from timezonefinder import TimezoneFinder


def read_folders_structure(file_path: str = "directories.yaml") -> dict[str, str]:
    """
    Read the folders structure from a yaml file.

    Parameters
    ----------
    file_path : str, optional
        The path to the yaml file containing the folders structure

    Returns
    -------
    folders_structure : dict of str
        The folders structure
    """

    # Read the folders structure from the file.
    with open(file_path, "r") as file:
        folders_structure = yaml.safe_load(file)

    return folders_structure


def read_countries_from_file(file_path: str) -> list[str]:
    """
    Read the countries from a file and get their ISO Alpha-2 codes.

    Parameters
    ----------
    file_path : str
        The path to the file containing the countries

    Returns
    -------
    iso_alpha_2_codes : list of str
        The ISO Alpha-2 codes of the countries
    """

    # Read the countries from the file.
    with open(file_path, "r") as file:
        countries = file.read().splitlines()

    iso_alpha_2_codes = []
    for country in countries:
        try:
            iso_alpha_2_codes.append(pycountry.countries.lookup(country).alpha_2)
        except LookupError:
            logging.error(f"{country} not found.")

    return iso_alpha_2_codes


def read_us_regions_from_file(file_path: str) -> list[str]:
    """
    Read the US regions from a file and get their codes.

    Parameters
    ----------
    file_path : str
        The path to the file containing the US regions

    Returns
    -------
    region_codes : list of str
        The codes of the US regions
    """

    # Read the US regions from the file.
    with open(file_path, "r") as file:
        us_regions = file.read().splitlines()

    # Define the codes of the regions.
    region_code_mapping = {
        "California": "CAL",
        "Carolinas": "CAR",
        "Central": "CENT",
        "Florida": "FLA",
        "Mid-Atlantic": "MIDA",
        "Midwest": "MIDW",
        "New England": "NE",
        "New York": "NY",
        "Northwest": "NW",
        "Southeast": "SE",
        "Southwest": "SW",
        "Tennessee": "TEN",
        "Texas": "TEX",
    }

    region_codes = []
    for region in us_regions:
        try:
            region_codes.append("US_" + region_code_mapping[region])
        except KeyError:
            logging.error(f"{region} not found.")

    return region_codes


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
            time_zone = tf.timezone_at(lat=location[0], lng=location[1])
        else:
            # Get the time zone of the country.
            time_zone = time_zones[0]

    else:
        # Split the country code into the ISO Alpha-2 code and the region code.
        iso_alpha_2_code, region_code = code.split("_")

        if iso_alpha_2_code == "US":
            time_zone = get_us_region_time_zone(iso_alpha_2_code + "_" + region_code)

    return time_zone
