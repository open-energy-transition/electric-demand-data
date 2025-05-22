# -*- coding: utf-8 -*-
"""
License: AGPL-3.0

Description:

    This script povides utility functions to read country and subdivision codes, time zones, and date ranges from yaml files.
"""

import datetime
import logging
import os

import pandas
import pycountry
import pytz
import util.directories
import yaml
from countryinfo import CountryInfo
from timezonefinder import TimezoneFinder


def _read_data_sources() -> list[str]:
    """
    Read the names of the data sources from the yaml files.

    Returns
    -------
    data_sources : list of str
        The names of the data sources
    """

    # Get the paths to the yaml files of the data sources.
    file_paths = util.directories.list_yaml_files("retrieval_scripts_folder")

    # Read the data sources from the file names.
    return [
        os.path.basename(file_path).split(".")[0].upper() for file_path in file_paths
    ]


def _read_entities_info(file_path: str = "", data_source: str = "") -> list[dict]:
    """
    Get the information of the countries and subdivisions in the yaml file of the data source.

    Parameters
    ----------
    file_path : str
        The path to the file containing the information of the countries and subdivisions
    data_source : str
        The name of the data source
    Returns
    -------
    list[dict[str, str | datetime.date]]
        The list of dictionaries containing the information for each country and subdivision
    """

    if data_source != "":
        # Make sure the data source is in uppercase.
        data_source = data_source.upper()

    if file_path == "" and data_source != "":
        # Check if the data source is valid.
        if data_source not in _read_data_sources():
            raise ValueError(
                f"Invalid data source: {data_source}. Available data sources are: {', '.join(_read_data_sources())}"
            )

        # Get the path to the yaml file of the data source.
        file_path = os.path.join(
            util.directories.read_folders_structure()["retrieval_scripts_folder"],
            f"{data_source.lower()}.yaml",
        )
    elif file_path == "" and data_source == "":
        # If no data source is provided, raise an error.
        raise ValueError("Either file_path or data_source must be provided.")
    elif file_path != "" and data_source != "":
        # If both data source and data source file path are provided, raise an error.
        raise ValueError("Only one of file_path or data_source must be provided.")

    # Read the content from the file.
    with open(file_path, "r") as file:
        content = yaml.safe_load(file)

    # Return the information of the countries and subdivisions.
    return content["entities"]


def read_codes(file_path: str = "", data_source: str = "") -> list[str]:
    """
    Read the codes of the countries and subdivisions in the yaml file of the data source.

    Parameters
    ----------
    file_path : str
        The path to the file containing the information of the countries and subdivisions
    data_source : str
        The name of the data source

    Returns
    -------
    codes : list of str
        The ISO Alpha-2 codes of the countries or the combination of the ISO Alpha-2 codes and the subdivision codes
    """

    # Extract the information of the countries and subdivisions.
    entities = _read_entities_info(file_path=file_path, data_source=data_source)

    # Extract codes.
    codes = [
        entity["country_code"] + "_" + entity["subdivision_code"]
        if "subdivision_code" in entity
        else entity["country_code"]
        for entity in entities
    ]

    return codes


def read_all_codes() -> list[str]:
    """
    Read the codes of all countries and subdivisions.

    Returns
    -------
    codes : list of str
        The ISO Alpha-2 codes of the countries or the combination of the ISO Alpha-2 codes and the subdivision codes
    """

    # Get the path of all yaml files in the retrieval scripts folder.
    yaml_file_paths = util.directories.list_yaml_files("retrieval_scripts_folder")

    # Define a list to store the codes.
    codes = []
    # Iterate over the yaml files and read the codes from each file.
    for yaml_file_path in yaml_file_paths:
        # Read the codes from the file.
        file_codes = read_codes(file_path=yaml_file_path)

        # Append the codes to the list.
        codes.extend(file_codes)

    # Remove duplicates.
    codes = list(set(codes))

    return codes


def check_code(code: str, data_source: str) -> None:
    """
    Check if the code is available in the yaml file of the data source.

    Parameters
    ----------
    code : str
        The code of the subdivision
    """

    # Check if the code is valid.
    assert code in read_codes(data_source=data_source), (
        f"Invalid code: {code}. Available codes are: {', '.join(read_codes(data_source=data_source))}"
    )


def check_and_get_codes(
    data_source: str | None = None,
    code: str | None = None,
    file_path: str | None = None,
) -> list[str]:
    """
    Check the validity of the country and subdivision codes and return the list of codes of interest.

    Parameters
    ----------
    data_source : str
        The name of the data source
    code : str
        The code of the country or subdivision
    file_path : str
        The path to the file containing the information of the countries and subdivisions

    Returns
    -------
    codes : list[str]
        The list of codes of the countries and subdivisions of interest
    """

    # Get the list of available countries and subdivisions according to the arguments.
    if data_source is not None:
        all_codes = read_codes(data_source=data_source)
    else:
        all_codes = read_all_codes()

    if code is not None:
        # Check if the code is available.
        if code not in all_codes:
            raise ValueError(
                f"Code {code} is not available. Please choose one of the following: {', '.join(all_codes)}"
            )
        else:
            codes = [code]

    elif file_path is not None:
        # Get the list of countries and subdivisions available on the specified file.
        codes = read_codes(file_path=file_path)

        # Check if the codes are available.
        for code in codes:
            if code not in all_codes:
                logging.error(f"Code {code} is not available.")
                codes.remove(code)

        # Check if there are any codes left.
        if len(codes) == 0:
            raise ValueError(
                f"None of the codes in the file are available. Please choose from the following: {', '.join(all_codes)}"
            )
    else:
        # If the file or code is not provided, use all the available codes.
        codes = all_codes

    return codes


def get_iso_alpha_3_code(code: str) -> str:
    """
    Get the ISO Alpha-3 code of the country from the ISO Alpha-2 code or the ISO Alpha-3 code of the country that contains the subdivision.

    Parameters
    ----------
    code : str
        The ISO Alpha-2 code of the country or the combination of the ISO Alpha-2 codes and the subdivision codes

    Returns
    -------
    iso_alpha_3_code : str
        The ISO Alpha-3 code of the country
    """

    # Check if the code specifies a subdivision.
    if "_" in code:
        # Extract the ISO Alpha-2 code of the country.
        iso_alpha_2_code = code.split("_")[0]
    else:
        # If the code does not specify a subdivision, use the ISO Alpha-2 code directly.
        iso_alpha_2_code = code

    # Get the ISO Alpha-3 code of the country.
    iso_alpha_3_code = pycountry.countries.get(alpha_2=iso_alpha_2_code).alpha_3

    return iso_alpha_3_code


def _get_country_time_zone(iso_alpha_2_code: str) -> pytz.timezone:
    """
    Get the time zone of a country.

    Parameters
    ----------
    iso_alpha_2_code : str
        The ISO Alpha-2 code of the country

    Returns
    -------
    time_zone : pytz.timezone
        The time zone of the country
    """

    # Define the time zones of countries that are not fully recognized.
    not_fully_recognized_countries = {
        "XK": ["Europe/Belgrade"]  # Kosovo
    }

    if "_" not in iso_alpha_2_code:
        # Get the list of time zones for the country.
        try:
            time_zones = pytz.country_timezones[iso_alpha_2_code]
        except KeyError:
            # If the country is not on pytz, try to get the time zone from the mapping dictionary of non-fully recognized countries.
            if iso_alpha_2_code in not_fully_recognized_countries:
                time_zones = not_fully_recognized_countries[iso_alpha_2_code]
            else:
                raise ValueError(f"Country code {iso_alpha_2_code} is not recognized.")

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
            time_zone = pytz.timezone(time_zones[0])
    else:
        raise ValueError(f"Expected ISO Alpha-2 code, but got {iso_alpha_2_code}.")

    return time_zone


def _get_time_zones(
    file_path: str = "", data_source: str = ""
) -> dict[str, pytz.timezone]:
    """
    Get the time zones of the countries and subdivisions in the yaml file of the data source.

    Parameters
    ----------
    file_path : str
        The path to the file containing the information of the countries and subdivisions
    data_source : str
        The name of the data source

    Returns
    -------
    time_zones : dict[str, pytz.timezone]
        The dictionary containing the time zones of the countries and subdivisions
    """

    # Extract the information of the countries and subdivisions.
    entities = _read_entities_info(file_path=file_path, data_source=data_source)

    # Define a dictionary to store the time zones of the countries and subdivisions.
    time_zones: dict[str, pytz.timezone] = {}

    # Iterate over the entities and extract the time zones.
    for entity in entities:
        # Extract the code of the country or subdivision.
        code = (
            entity["country_code"] + "_" + entity["subdivision_code"]
            if "subdivision_code" in entity
            else entity["country_code"]
        )

        # If the code specifies a subdivision, extract the time zone from the file.
        if "time_zone" in entity and "_" in code:
            time_zone = pytz.timezone(entity["time_zone"])
        # If the code specifies a country, get the time zone based on the country code.
        elif "time_zone" not in entity and "_" not in code:
            time_zone = _get_country_time_zone(code)
        # If the code specifies a subdivision and the time zone is also defined in the file, check if the time zone is the same as the one in the file.
        elif "time_zone" in entity and "_" not in code:
            time_zone = _get_country_time_zone(code)
            # Check if the time zone is the same as the one in the file.
            if time_zone.zone != entity["time_zone"]:
                raise ValueError(
                    f"The time zone in the file does not match the expected time zone for {code}."
                )
        # If the code specifies a subdivision and the time zone is not defined in the file, raise an error.
        else:
            raise ValueError(f"The time zone is not defined for {code}.")

        # Add the code to the dictionary and the respective time zone.
        time_zones[code] = time_zone

    return time_zones


def get_all_time_zones() -> dict[str, pytz.timezone]:
    """
    Get the time zones of all countries and subdivisions.

    Returns
    -------
    time_zones : dict[str, pytz.timezone]
        The dictionary containing the time zones of the countries and subdivisions
    """

    # Get the path to all yaml files in the retrieval scripts folder.
    yaml_file_paths = util.directories.list_yaml_files("retrieval_scripts_folder")

    # Define a dictionary to store the time zones of the countries and subdivisions.
    time_zones: dict[str, pytz.timezone] = {}

    # Iterate over the yaml files and read the time zones from each file.
    for yaml_file_path in yaml_file_paths:
        # Read the time zones from the file.
        file_time_zones = _get_time_zones(file_path=yaml_file_path)

        # Check for duplicates.
        for code, time_zone in file_time_zones.items():
            # If the code is already in the dictionary, check if the time zone is the same as the one in the file.
            if code in time_zones:
                if time_zones[code].zone != time_zone.zone:
                    raise ValueError(
                        f"The time zone in the file {os.path.basename(yaml_file_path)} does not match the previously defined time zone for {code}."
                    )
            else:
                # If the code is not in the dictionary, add it to the dictionary.
                time_zones[code] = time_zone

    return time_zones


def get_time_zone(code: str) -> pytz.timezone:
    """
    Get the time zone of a country or subdivision.

    Parameters
    ----------
    code : str
        The ISO Alpha-2 code of the country or the combination of the ISO Alpha-2 codes and the subdivision codes

    Returns
    -------
    time_zone : pytz.timezone
        The time zone of the country or subdivision
    """

    # Get the time zones of all countries and subdivisions.
    time_zones = get_all_time_zones()

    # Check if the code is in the dictionary.
    if code in time_zones:
        # If the code is in the dictionary, return the time zone.
        return time_zones[code]
    else:
        # If the code is not in the dictionary, raise an error.
        raise ValueError(
            f"{code} is not among the available countries and subdivisions."
        )


def read_date_ranges(
    file_path: str = "", data_source: str = ""
) -> dict[str, tuple[datetime.date, datetime.date]]:
    """
    Read the start and end dates of the available data for the countries and subdivisions in the yaml file of the data source.

    Parameters
    ----------
    file_path : str
        The path to the file containing the information of the countries and subdivisions
    data_source : str
        The name of the data source

    Returns
    -------
    start_date : datetime.date
        The start date of the available data
    end_date : datetime.date
        The end date of the available data
    """

    # Extract the information of the countries and subdivisions.
    entities = _read_entities_info(file_path=file_path, data_source=data_source)

    # Define a dictionary to store the start and end dates of the available data for the countries and subdivisions.
    start_and_end_dates: dict[str, tuple[datetime.date, datetime.date]] = {}

    # Iterate over the entities and extract the start and end dates.
    for entity in entities:
        # Extract the code of the country or subdivision.
        code = (
            entity["country_code"] + "_" + entity["subdivision_code"]
            if "subdivision_code" in entity
            else entity["country_code"]
        )

        # Extract the start and end dates of the available data.
        start_date = entity["start_date"]
        if entity["end_date"] == "today":
            # If the end date is "today", set it to a few days before today to avoid issues with the latest data.
            end_date = (datetime.datetime.today() - datetime.timedelta(days=5)).date()
        else:
            end_date = entity["end_date"]

        # Add the code to the dictionary and the respective start and end dates.
        start_and_end_dates[code] = (start_date, end_date)

        # Check if the start date is before the end date.
        if start_date > end_date:
            raise ValueError(
                f"The start date {start_date} is after the end date {end_date} for {code}."
            )

    return start_and_end_dates


def read_all_date_ranges() -> dict[str, tuple[datetime.date, datetime.date]]:
    """
    Read the start and end dates of the available data for all countries and subdivisions.

    Returns
    -------
    start_and_end_dates : dict[str, tuple[datetime.date, datetime.date]]
        The dictionary containing the start and end dates of the available data for the countries and subdivisions
    """

    # Get the path to all yaml files in the retrieval scripts folder.
    yaml_file_paths = util.directories.list_yaml_files("retrieval_scripts_folder")

    # Define a dictionary to store the start and end dates of the available data for the countries and subdivisions.
    start_and_end_dates: dict[str, tuple[datetime.date, datetime.date]] = {}

    # Iterate over the yaml files and read the start and end dates from each file.
    for yaml_file_path in yaml_file_paths:
        # Read the start and end dates from the file.
        file_start_and_end_dates = read_date_ranges(file_path=yaml_file_path)

        # Check for duplicates.
        for code, (start_date, end_date) in file_start_and_end_dates.items():
            # If the code is already in the dictionary, get the dates giving the longest period.
            if code in start_and_end_dates:
                existing_start_date, existing_end_date = start_and_end_dates[code]
                existing_time_difference = (
                    existing_end_date - existing_start_date
                ).days
                new_time_difference = (end_date - start_date).days
                if new_time_difference > existing_time_difference:
                    # If the new time difference is greater, update the start and end dates.
                    start_and_end_dates[code] = (start_date, end_date)
            else:
                # If the code is not in the dictionary, add it to the dictionary.
                start_and_end_dates[code] = (start_date, end_date)

    return start_and_end_dates


def get_available_years(code: str) -> list[int]:
    """
    Get the years of the available data for a country or subdivision.

    Parameters
    ----------
    code : str
        The ISO Alpha-2 code of the country or the combination of the ISO Alpha-2 and the subdivision code

    Returns
    -------
    years : list[int]
        The years of the available data for the country or subdivision
    """

    # Read the start and end dates of the available data for the country or subdivision of interest.
    start_date, end_date = read_all_date_ranges()[code]

    # Get the time zone of the country or subdivision.
    entity_time_zone = get_time_zone(code)

    # Convert the start and end dates to the time zone of the country or subdivision.
    start_date = (
        pandas.to_datetime(start_date).tz_localize(entity_time_zone).tz_convert("UTC")
    )
    end_date = (
        pandas.to_datetime(end_date).tz_localize(entity_time_zone).tz_convert("UTC")
    )

    # Return the years of the data availability.
    return [year for year in range(start_date.year, end_date.year + 1)]
