# -*- coding: utf-8 -*-
"""
License: AGPL-3.0.

Description:

    This file contains unit tests for the entities module in the ETL
    utility package.
"""

import datetime
from unittest.mock import mock_open, patch

import pytest
import pytz
import utils.entities


def test_code():
    """
    Test if the entities module can read codes correctly.

    This test checks if the functions in the entities module can
    correctly read codes from a yaml file or from a specific data
    source, and if they handle errors correctly.
    """
    # Define a sample yaml file content.
    sample_yaml = """
    entities:
      - country_name: France
        country_code: FR
        start_date: 2014-12-15
        end_date: today
      - subdivision_name: Texas
        subdivision_code: TEX
        country_name: United States
        country_code: US
        start_date: 2020-01-01
        end_date: today
        time_zone: America/Chicago
    """

    # Read the codes from the sample yaml file and check them.
    with patch("builtins.open", mock_open(read_data=sample_yaml)):
        sample_codes = utils.entities.read_codes(file_path="dummy.yaml")
    assert sample_codes == ["FR", "US_TEX"]

    # Read codes belonging to a specific data source and check them.
    entsoe_codes = utils.entities.read_codes(data_source="ENTSOE")
    assert isinstance(entsoe_codes, list)
    assert "FR" in entsoe_codes
    assert "US_TEX" not in entsoe_codes

    # Read codes belonging to a specific data source and check them.
    entsoe_codes = utils.entities.check_and_get_codes(data_source="ENTSOE")
    assert isinstance(entsoe_codes, list)
    assert "FR" in entsoe_codes
    assert "US_TEX" not in entsoe_codes

    # Read all codes available in the yaml files and check them.
    all_codes = utils.entities.read_all_codes()
    assert isinstance(all_codes, list)
    assert "FR" in all_codes
    assert "US_TEX" in all_codes

    # Check code validity with a specific code.
    assert utils.entities.check_and_get_codes(
        code="FR", data_source="ENTSOE"
    ) == ["FR"]
    assert utils.entities.check_and_get_codes(
        code="US_TEX", data_source="EIA"
    ) == ["US_TEX"]

    # Define a mock for the check_and_get_codes function to simulate
    # reading codes from a file path.
    dummy_codes_in_file = ["FR", "DE"]
    all_dummy_codes = ["FR", "DE", "IT"]

    # Read codes from a specified file path and check them.
    with (
        patch("utils.entities.read_codes", return_value=dummy_codes_in_file),
        patch("utils.entities.read_all_codes", return_value=all_dummy_codes),
    ):
        dummy_codes = utils.entities.check_and_get_codes(
            file_path="dummy.yaml"
        )

        # Check if the codes for a specific file are read correctly.
        assert isinstance(dummy_codes, list)
        assert "FR" in dummy_codes
        assert "US_TEX" not in dummy_codes

    # Check if ISO alpha-3 codes are read correctly.
    assert utils.entities.get_iso_alpha_3_code("FR") == "FRA"
    assert utils.entities.get_iso_alpha_3_code("US_TEX") == "USA"

    # Check if common errors in input data are handled correctly.
    with pytest.raises(ValueError):
        utils.entities.read_codes(file_path="", data_source="")
    with pytest.raises(ValueError):
        utils.entities.read_codes(
            file_path="INVALID_DATA_SOURCE", data_source="INVALID_DATA_SOURCE"
        )
    with pytest.raises(ValueError):
        utils.entities.read_codes(data_source="INVALID_DATA_SOURCE")
    utils.entities.check_code("FR", data_source="ENTSOE")
    utils.entities.check_code("US_TEX", data_source="EIA")
    with pytest.raises(AssertionError):
        utils.entities.check_code("INVALID_CODE", data_source="ENTSOE")
    with pytest.raises(ValueError):
        utils.entities.check_code("FR", data_source="INVALID_DATA_SOURCE")
    with pytest.raises(ValueError):
        utils.entities.check_and_get_codes(
            code="INVALID_CODE", data_source="ENTSOE"
        )
    # Define a mock for the check_and_get_codes function to check if it
    # raises an error when the codes in the file do not match the
    # expected codes.
    dummy_unknown_codes_in_file = ["US_CAL", "US_TEX"]
    with pytest.raises(ValueError):
        with (
            patch(
                "utils.entities.read_codes",
                return_value=dummy_unknown_codes_in_file,
            ),
            patch(
                "utils.entities.read_all_codes", return_value=all_dummy_codes
            ),
        ):
            __ = utils.entities.check_and_get_codes(file_path="dummy.yaml")
    with pytest.raises(ValueError):
        utils.entities.get_iso_alpha_3_code("INVALID_CODE")


def test_time_zones():
    """
    Test if the entities module can retrieve time zones correctly.

    This test checks if the entities module can retrieve the time
    zones for countries and subdivisions, and if it raises an error
    for invalid codes. It also checks if the time zones are correctly
    set for the specified codes.
    """
    # Define sample yaml file content with invalid data.
    sample_yaml_with_invalid_time_zone = """
    entities:
      - country_name: France
        country_code: FR
        start_date: 2014-12-15
        end_date: today
        time_zone: America/Chicago
    """
    sample_yaml_with_missing_time_zone = """
    entities:
      - subdivision_name: Texas
        subdivision_code: TEX
        country_name: United States
        country_code: US
        start_date: 2020-01-01
        end_date: today
    """

    # Check the time zone of a country.
    assert utils.entities.get_time_zone("FR") == pytz.timezone("Europe/Paris")

    # Check the time zone of a subdivision.
    assert utils.entities.get_time_zone("US_CAL") == pytz.timezone(
        "America/Los_Angeles"
    )

    # Invalid code should raise an error.
    with pytest.raises(ValueError):
        utils.entities.get_time_zone("INVALID_CODE")
    with pytest.raises(ValueError):
        utils.entities._get_country_time_zone("INVALID_CODE")
    with pytest.raises(ValueError):
        utils.entities._get_country_time_zone("INVALIDCODE")
    with pytest.raises(ValueError):
        with patch(
            "builtins.open",
            mock_open(read_data=sample_yaml_with_invalid_time_zone),
        ):
            utils.entities._get_time_zones("dummy.yaml")
    with pytest.raises(ValueError):
        with patch(
            "builtins.open",
            mock_open(read_data=sample_yaml_with_missing_time_zone),
        ):
            utils.entities._get_time_zones("dummy.yaml")

    # Define sample yaml file content to simulate reading conflicting
    # time zones from two yaml files.
    mock_file_paths = ["file1.yaml", "file2.yaml"]

    # Mock return values from _get_time_zones.
    tz1 = {"US": pytz.timezone("America/New_York")}
    tz2 = {"US": pytz.timezone("America/Los_Angeles")}

    with (
        patch(
            "utils.directories.list_yaml_files", return_value=mock_file_paths
        ),
        patch("utils.entities._get_time_zones", side_effect=[tz1, tz2]),
    ):
        with pytest.raises(ValueError):
            utils.entities.get_all_time_zones()


def test_date_ranges():
    """
    Test if the date ranges are read correctly.

    This test checks if the function reads the date ranges from a yaml
    file and returns a dictionary with the expected keys and values.
    """
    # Define a sample yaml file content.
    sample_yaml = """
    entities:
      - country_name: France
        country_code: FR
        start_date: 2014-12-15
        end_date: today
      - subdivision_name: Texas
        subdivision_code: TEX
        country_name: United States
        country_code: US
        start_date: 2020-01-01
        end_date: today
        time_zone: America/Chicago
    """
    # Define sample yaml file content with an invalid data.
    sample_yaml_with_invalid_date_range = """
    entities:
      - country_name: France
        country_code: FR
        start_date: 2014-12-15
        end_date: 2012-01-01
    """

    # Read the date ranges from the sample yaml file.
    with patch("builtins.open", mock_open(read_data=sample_yaml)):
        date_ranges = utils.entities.read_date_ranges(file_path="dummy.yaml")

    # Check if function returns a list of date ranges.
    assert isinstance(date_ranges, dict)

    # Check if the date ranges are read correctly.
    assert date_ranges["FR"] == (
        datetime.date(2014, 12, 15),
        (datetime.datetime.today() - datetime.timedelta(days=5)).date(),
    )

    # Check if the function raises an error for invalid date ranges.
    with pytest.raises(ValueError):
        with patch(
            "builtins.open",
            mock_open(read_data=sample_yaml_with_invalid_date_range),
        ):
            utils.entities.read_date_ranges(file_path="dummy.yaml")


def test_years():
    """
    Test if the function retrieves available years correctly.

    This test checks if the function retrieves the available years for a
    specific country code and returns a list of years.
    """
    # Check if the function retrieves available years for a specific
    # country code
    years = utils.entities.get_available_years("FR")

    # Check if the years are read correctly.
    assert isinstance(years, list)
    assert len(years) > 0
    assert all(isinstance(year, int) for year in years)

    # Check if the function catches errors for invalid codes.
    with pytest.raises(ValueError):
        utils.entities.get_available_years("INVALID_CODE")


def test_continents():
    """
    Test if the function retrieves continents correctly.

    This test checks if the function retrieves the continent for a
    specific country code and returns the expected continent.
    """
    # Check if the function retrieves the continent for a specific
    # country code.
    assert utils.entities.get_continent_code("FR") == "EU"
    assert utils.entities.get_continent_code("US_TEX") == "NA"

    # Check if the function catches errors for invalid codes.
    with pytest.raises(ValueError):
        utils.entities.get_continent_code("INVALID_CODE")
