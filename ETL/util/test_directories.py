import os
from unittest.mock import patch

import util.directories


def test_load_paths():
    # Read the folders structure from the sample yaml file.
    structure = util.directories.read_folders_structure()

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
    folder_name = "config"
    target_path = "/mocked/path/config"

    mock_read_folders_structure.return_value = {folder_name: target_path}
    mock_listdir.return_value = ["file1.yaml", "file2.yaml", "ignore.txt"]

    expected = [
        os.path.join(target_path, "file1.yaml"),
        os.path.join(target_path, "file2.yaml"),
    ]

    result = util.directories.list_yaml_files(folder_name)
    assert result == expected, f"Expected {expected}, got {result}"
