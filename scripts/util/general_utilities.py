import pandas as pd
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
    country_codes : list of str
        The ISO Alpha-2 codes of the countries
    """

    # Read the countries from the file.
    with open(file_path, "r") as file:
        countries = file.read().splitlines()

    country_codes = []
    for country in countries:
        try:
            country_codes.append(pycountry.countries.lookup(country).alpha_2)
        except LookupError:
            print(f"{country} not found.")

    return country_codes


def get_time_zone(country_code: str) -> pytz.timezone:
    """
    Get the time zone of a country.

    Parameters
    ----------
    country_code : str
        The ISO Alpha-2 code of the country

    Returns
    -------
    time_zone : pytz.timezone
        The time zone of the country or location
    """

    # Get the list of time zones for the country.
    time_zones = pytz.country_timezones[country_code]

    # If there are multiple time zones, find the time zone based on the capital city.
    if len(time_zones) > 1:
        # Get the country name.
        country = pycountry.countries.get(alpha_2=country_code)

        # Get the capital city coordinates.
        location = CountryInfo(country.name).capital_latlng()

        # Find time zone based on capital city coordinates.
        tf = TimezoneFinder()
        time_zone = tf.timezone_at(lat=location[0], lng=location[1])
    else:
        # Get the time zone of the country.
        time_zone = time_zones[0]

    return time_zone


def calculate_time_difference(
    local_time_zone: pytz.timezone, year: int = 2020
) -> float:
    """
    Calculate the time shift between UTC and the local time zone.

    Parameters
    ----------
    local_time_zone : pytz.timezone
        Local time zone
    year : int, optional
        The year for which the time difference is calculated

    Returns
    -------
    time_difference : float
        Time difference in hours
    """

    # Get an arbitrary time expressed both in UTC and the local time zone.
    datetime1 = pd.Timestamp(f"{year}-01-01 00:00:00", tz="UTC")
    datetime2 = pd.Timestamp(f"{year}-01-01 00:00:00", tz=local_time_zone)

    # Calculate the time shift between UTC and the local time zone.
    time_difference = (datetime2 - datetime1).total_seconds() / 3600

    return time_difference
