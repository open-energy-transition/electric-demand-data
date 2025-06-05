# -*- coding: utf-8 -*-
"""
License: AGPL-3.0.

Description:

    This file contains unit tests for the entities module in the ETL utility package.
"""

import datetime
from unittest.mock import mock_open, patch

import pytest
import pytz
import util.entities


@pytest.fixture
def sample_yaml():
    """
    Fixture to provide a sample yaml file content for testing. This yaml file contains a list of entities, including countries and subdivisions, with their respective codes, start dates, end dates, and time zones.

    Returns
    -------
    str
        A string representation of a yaml file containing entities information.
    """
    return """
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


def test_read_entities_info(sample_yaml):
    """
    Test if the entities information is read correctly from the sample yaml file.

    Parameters
    ----------
    sample_yaml : str
        A string representation of a yaml file containing entities information.
    """
    # Read the entities info from the sample yaml file.
    with patch("builtins.open", mock_open(read_data=sample_yaml)):
        entities = util.entities._read_entities_info(file_path="dummy.yaml")

    # Check if function returns a list of entities.
    assert isinstance(entities, list)

    # Check if each entity is a dictionary with the expected keys.
    for entity in entities:
        assert isinstance(entity, dict)
        assert "country_name" in entity
        assert "country_code" in entity
        assert "start_date" in entity
        assert "end_date" in entity

    # Check if entities that are subdivisions have the expected keys.
    for entity in entities:
        if "subdivision_name" in entity:
            assert "subdivision_code" in entity
            assert "country_name" in entity
            assert "country_code" in entity
            assert "start_date" in entity
            assert "end_date" in entity
            assert "time_zone" in entity


def test_read_codes(sample_yaml):
    """
    Test if the codes are read correctly from the sample yaml file.

    Parameters
    ----------
    sample_yaml : str
        A string representation of a yaml file containing entities information.
    """
    # Read the codes from the sample yaml file.
    with patch("builtins.open", mock_open(read_data=sample_yaml)):
        codes = util.entities.read_codes(file_path="dummy.yaml")

    # Check if function returns a list of codes.
    assert isinstance(codes, list)

    # Check if the codes are read correctly.
    assert codes == ["FR", "US_TEX"]


def test_check_code():
    """Test if the function correctly identifies valid and invalid codes."""
    # Check if the function correctly identifies valid and invalid codes.
    util.entities.check_code("FR", data_source="ENTSOE")
    util.entities.check_code("US_TEX", data_source="EIA")
    with pytest.raises(AssertionError):
        util.entities.check_code("INVALID_CODE", data_source="ENTSOE")
    with pytest.raises(ValueError):
        util.entities.check_code("FR", data_source="INVALID_DATA_SOURCE")


def test_get_time_zone():
    """Test if the function correctly retrieves the time zone for a country or subdivision."""
    # Check the time zone of a country.
    assert util.entities.get_time_zone("FR") == pytz.timezone("Europe/Paris")

    # Check the time zone of a subdivision.
    assert util.entities.get_time_zone("US_CAL") == pytz.timezone("America/Los_Angeles")

    # Invalid code should raise an error.
    with pytest.raises(ValueError):
        util.entities.get_time_zone("INVALID_CODE")


def test_read_date_ranges(sample_yaml):
    """
    Test if the date ranges are read correctly from the sample yaml file.

    Parameters
    ----------
    sample_yaml : str
        A string representation of a yaml file containing entities information.
    """
    # Read the date ranges from the sample yaml file.
    with patch("builtins.open", mock_open(read_data=sample_yaml)):
        date_ranges = util.entities.read_date_ranges(file_path="dummy.yaml")

    # Check if function returns a list of date ranges.
    assert isinstance(date_ranges, dict)

    # Check if the date ranges are read correctly.
    assert date_ranges["FR"] == (
        datetime.date(2014, 12, 15),
        (datetime.datetime.today() - datetime.timedelta(days=5)).date(),
    )
