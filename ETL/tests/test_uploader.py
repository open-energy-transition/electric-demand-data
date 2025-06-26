# -*- coding: utf-8 -*-
"""
License: AGPL-3.0.

Description:

    This file contains unit tests for the uploader module in the ETL
    utility package.
"""

from unittest.mock import MagicMock, mock_open, patch

import pytest
import requests
import utils.uploader
from google.cloud.exceptions import GoogleCloudError


def test_upload_to_gcs():
    """
    Test the upload_to_gcs function by mocking GCS interactions.

    This test checks if the function correctly uploads a file to a
    Google Cloud Storage (GCS) bucket. It uses mocking to simulate the
    GCS interactions, ensuring that the function behaves as expected
    without requiring actual GCS access. It also tests the error
    handling for OSError and GoogleCloudError.
    """
    # Define a mock for the GCS bucket and blob.
    mock_bucket = MagicMock()
    mock_blob = MagicMock()

    with patch("utils.uploader.storage.Client") as mock_storage_client:
        # Mock the bucket and blob.
        mock_bucket.blob.return_value = mock_blob
        mock_storage_client.return_value.bucket.return_value = mock_bucket

        # Call the function.
        utils.uploader.upload_to_gcs(
            "dummy/path.txt", "test-bucket", "dest/path.txt"
        )

        # Assert the file was uploaded.
        mock_blob.upload_from_filename.assert_called_once_with(
            "dummy/path.txt"
        )

        # Check if the OSError is handled correctly.
        mock_blob.upload_from_filename.side_effect = OSError("file not found")
        with pytest.raises(OSError):
            utils.uploader.upload_to_gcs(
                "bad/path.txt", "test-bucket", "dest/path.txt"
            )

        # Check if the GoogleCloudError is handled correctly.
        mock_blob.upload_from_filename.side_effect = GoogleCloudError(
            "GCS failure"
        )
        with pytest.raises(GoogleCloudError):
            utils.uploader.upload_to_gcs(
                "bad/path.txt", "test-bucket", "dest/path.txt"
            )


def test_upload_file_to_new_deposition_on_zenodo():
    """
    Test upload_to_zenodo for creating a new deposition.

    This test checks if the function correctly creates a new deposition
    on Zenodo and uploads a file to it. It uses mocking to simulate the
    Zenodo API interactions.
    """
    with (
        patch("requests.get") as mock_get,
        patch("requests.post") as mock_post,
        patch("utils.directories.read_folders_structure") as mock_read_folders,
        patch("dotenv.load_dotenv") as mock_load_dotenv,
        patch("os.getenv") as mock_getenv,
        patch("builtins.open", mock_open(read_data="test data")),
    ):
        # Mock the folders structure to return a specific structure.
        mock_read_folders.return_value = {"root_folder": "fake/root"}

        # Mock the environment variable for Zenodo token.
        mock_load_dotenv.return_value = True
        mock_getenv.return_value = "fake_zenodo_token"

        # Mock the response for getting depositions.
        mock_get.return_value = requests.Response()
        mock_get.return_value.status_code = 200
        mock_get.return_value.json = lambda: []

        # Mock the response for creating a new deposition.
        mock_post_1 = requests.Response()
        mock_post_1.status_code = 201
        mock_post_1.json = lambda: {"id": "12345"}

        # Mock the response for uploading a new file in the deposition.
        mock_post_2 = requests.Response()
        mock_post_2.status_code = 201

        # Mock the response for publishing the deposition.
        mock_post_3 = requests.Response()
        mock_post_3.status_code = 202

        # Combine the mock responses for the post requests.
        mock_post.side_effect = [mock_post_1, mock_post_2, mock_post_3]

        # Call the function to upload a file to a new deposition.
        utils.uploader.upload_to_zenodo(
            "/fake/root/file1.csv", "actual", publish=True, testing=True
        )


def test_upload_file_to_draft_version_on_zenodo():
    """
    Test upload_to_zenodo for uploading a file to a draft version.

    This test checks if the function correctly uploads a file to an
    existing draft of a new version of a deposition on Zenodo.
    """
    with (
        patch("requests.get") as mock_get,
        patch("requests.put") as mock_put,
        patch("requests.post") as mock_post,
        patch("utils.directories.read_folders_structure") as mock_read_folders,
        patch("dotenv.load_dotenv") as mock_load_dotenv,
        patch("os.getenv") as mock_getenv,
        patch("builtins.open", mock_open(read_data="test data")),
    ):
        # Mock the folders structure to return a specific structure.
        mock_read_folders.return_value = {"root_folder": "fake/root"}

        # Mock the environment variable for Zenodo token.
        mock_load_dotenv.return_value = True
        mock_getenv.return_value = "fake_zenodo_token"

        # Mock the response for getting depositions.
        mock_get.return_value = requests.Response()
        mock_get.return_value.status_code = 200
        mock_get.return_value.json = lambda: [
            {
                "id": "12345",
                "metadata": {
                    "title": (
                        "Global Dataset of Hourly or Sub-Hourly Historical "
                        "Electricity Demand"
                    )
                },
                "submitted": False,
            }
        ]

        # Mock the response for updating the deposition metadata.
        mock_put.return_value = requests.Response()
        mock_put.return_value.status_code = 200

        # Mock the response for uploading a new file in the deposition.
        mock_post.return_value = requests.Response()
        mock_post.return_value.status_code = 201

        # Call the function to upload a file to a draft version.
        utils.uploader.upload_to_zenodo(
            "/fake/root/file1.csv", "actual", publish=False, testing=True
        )


def test_upload_file_to_new_version_on_zenodo():
    """
    Test upload_to_zenodo for uploading a file to a new version.

    This test checks if the function correctly uploads a file to a new
    version of a deposition on Zenodo.
    """
    with (
        patch("requests.get") as mock_get,
        patch("requests.put") as mock_put,
        patch("requests.post") as mock_post,
        patch("requests.delete") as mock_delete,
        patch("utils.directories.read_folders_structure") as mock_read_folders,
        patch("dotenv.load_dotenv") as mock_load_dotenv,
        patch("os.getenv") as mock_getenv,
        patch("builtins.open", mock_open(read_data="test data")),
    ):
        # Mock the folders structure to return a specific structure.
        mock_read_folders.return_value = {"root_folder": "fake/root"}

        # Mock the environment variable for Zenodo token.
        mock_load_dotenv.return_value = True
        mock_getenv.return_value = "fake_zenodo_token"

        # Mock the response for getting depositions.
        mock_get_1 = requests.Response()
        mock_get_1.status_code = 200
        mock_get_1.json = lambda: [
            {
                "id": "12345",
                "metadata": {
                    "title": (
                        "Global Dataset of Hourly Synthetic Electricity Demand"
                    )
                },
                "submitted": True,
            }
        ]

        # Mock the response for creating a new deposition.
        mock_post_1 = requests.Response()
        mock_post_1.status_code = 201
        mock_post_1.json = lambda: {"id": "67890"}

        # Mock the response for updating the deposition metadata.
        mock_put.return_value = requests.Response()
        mock_put.return_value.status_code = 200

        # Mock the response for getting the files in the deposition.
        mock_get_2 = requests.Response()
        mock_get_2.status_code = 200
        mock_get_2.json = lambda: [{"id": "file"}]

        # Mock the response for deleting the existing files in the
        # deposition.
        mock_delete.return_value = requests.Response()
        mock_delete.return_value.status_code = 204

        # Mock the response for uploading a new file in the deposition.
        mock_post_2 = requests.Response()
        mock_post_2.status_code = 201

        # Combine the mock responses for the get requests.
        mock_get.side_effect = [mock_get_1, mock_get_2]

        # Combine the mock responses for the post requests.
        mock_post.side_effect = [mock_post_1, mock_post_2]

        # Call the function to upload a file to a new version.
        utils.uploader.upload_to_zenodo(
            "/fake/root/file1.csv", "synthetic", publish=False, testing=False
        )


def test_zenodo_errors():
    """
    Test error handling in upload_to_zenodo.

    This test checks if the function raises appropriate exceptions for
    invalid inputs and API errors.
    """
    with pytest.raises(ValueError):
        utils.uploader.upload_to_zenodo(
            "/fake/root/file1.csv",
            "invalid_type",
            publish=False,
            testing=False,
        )

    with (
        patch("requests.get") as mock_get,
        patch("utils.directories.read_folders_structure") as mock_read_folders,
        patch("dotenv.load_dotenv") as mock_load_dotenv,
        patch("os.getenv") as mock_getenv,
    ):
        # Mock the folders structure to return a specific structure.
        mock_read_folders.return_value = {"root_folder": "fake/root"}

        # Mock the environment variable for Zenodo token.
        mock_load_dotenv.return_value = True
        mock_getenv.return_value = "fake_zenodo_token"

        # Mock the response for getting depositions to simulate an
        # error.
        mock_get.return_value = requests.Response()
        mock_get.return_value.status_code = 500

        with pytest.raises(Exception):
            utils.uploader.upload_to_zenodo(
                "/fake/root/file1.csv", "actual", publish=False, testing=True
            )
