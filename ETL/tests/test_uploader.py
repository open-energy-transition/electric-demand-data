# -*- coding: utf-8 -*-
"""
License: AGPL-3.0.

Description:

    This file contains unit tests for the uploader module in the ETL
    utility package.
"""

from unittest.mock import MagicMock, patch

import pytest
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
