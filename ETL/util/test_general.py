from unittest.mock import mock_open, patch

import pytest
import pytz
from general import (
    get_time_zone,
    get_us_region_time_zone,
    read_codes_from_file,
    read_folders_structure,
)


@pytest.fixture
def sample_country_yaml():
    # Define a sample yaml file that contains countries and regions.
    return """
    items:
      - country_name: France
        country_code: FR
      - country_name: United Kingdom
        country_code: GB
      - region_name: Florida
        region_code: FLA
        country_name: United States
        country_code: US
      - region_name: Texas
        region_code: TEX
        country_name: United States
        country_code: US
    """


@pytest.fixture
def sample_directory_yaml():
    # Define a sample yaml file that contains folders and paths.
    return """
    folder1: path1
    folder2: path2
    """


def test_read_codes_from_file(sample_country_yaml):
    with patch("builtins.open", mock_open(read_data=sample_country_yaml)):
        # Read the codes from the sample yaml file.
        codes = read_codes_from_file("dummy.yaml")

        # Check if the codes are read correctly.
        assert codes == ["FR", "GB", "US_FLA", "US_TEX"]


def test_read_folders_structure(sample_directory_yaml):
    with patch("builtins.open", mock_open(read_data=sample_directory_yaml)):
        # Read the folders structure from the sample yaml file.
        structure = read_folders_structure("dummy.yaml")

        # Check if the folders structure is read correctly.
        assert structure == {"folder1": "path1", "folder2": "path2"}


def test_get_us_region_time_zone():
    # Check the time zones of the US regions.
    assert get_us_region_time_zone("US_CAL") == pytz.timezone("America/Los_Angeles")
    assert get_us_region_time_zone("US_TEX") == pytz.timezone("America/Chicago")


def test_get_time_zone():
    # Check the time zones of the countries and regions.
    # Single country and single timezone
    assert get_time_zone("FR") in pytz.country_timezones["FR"]

    # Country with multiple timezones, return timezone of capital
    assert get_time_zone("US") == pytz.timezone("America/New_York")

    # Country with multiple timezones, return timezone of region
    assert get_time_zone("US_CAL") == pytz.timezone("America/Los_Angeles")
