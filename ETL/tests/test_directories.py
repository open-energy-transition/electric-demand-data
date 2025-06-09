# -*- coding: utf-8 -*-
"""
License: AGPL-3.0.

Description:

    This file contains unit tests for the directories module in the ETL
    utility package.
"""

import os
from unittest.mock import patch

import utils.directories


def test_load_paths():
    """
    Test if the folders structure is read correctly.

    This test checks if the keys and values in the yaml file are read
    correctly and if the absolute paths are constructed as expected.
    """
    # Read the folders structure from the sample yaml file.
    structure = utils.directories.read_folders_structure()

    # Get the root path of the ETL folder.
    absolute_path = os.path.abspath(
        os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
    )

    # Check if the folders are read correctly.
    assert structure["data_folder"] == os.path.join(absolute_path, "data")
    assert structure["electricity_demand_folder"] == os.path.join(
        absolute_path, "data", "electricity_demand"
    )


@patch("util.directories.read_folders_structure")
@patch("util.directories.os.listdir")
def test_list_yaml_files(mock_listdir, mock_read_folders_structure):
    """
    Test if the yaml files in a specified folder are listed correctly.

    This test checks if the function correctly lists all yaml files in a
    specified folder, ignoring non-yaml files. It uses mocks to simulate
    the behavior of the `os.listdir` function and the
    `read_folders_structure` function, allowing the test to run without
    needing actual files or directories.

    Parameters
    ----------
    mock_listdir : unittest.mock.Mock
        Mock for the os.listdir function.
    mock_read_folders_structure : unittest.mock.Mock
        Mock for the read_folders_structure function.
    """
    # Define the folder name and target path for the test.
    folder_name = "config"
    target_path = "/mocked/path/config"

    # Mock the return values of the read_folders_structure and
    # os.listdir functions.
    mock_read_folders_structure.return_value = {folder_name: target_path}
    mock_listdir.return_value = ["file1.yaml", "file2.yaml", "ignore.txt"]

    # Define the expected result.
    expected = [
        os.path.join(target_path, "file1.yaml"),
        os.path.join(target_path, "file2.yaml"),
    ]

    # Call the function to test.
    result = utils.directories.list_yaml_files(folder_name)

    # Check if the result matches the expected output.
    assert result == expected, f"Expected {expected}, got {result}"
