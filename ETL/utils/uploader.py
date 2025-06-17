# -*- coding: utf-8 -*-
"""
License: AGPL-3.0.

Description:

    This module provides functions to upload files to Google Cloud
    Storage (GCS) and Zenodo.
"""

import logging

from google.cloud import storage
from google.cloud.exceptions import GoogleCloudError


def upload_to_gcs(
    file_path: str,
    bucket_name: str,
    destination_blob_name: str,
) -> None:
    """
    Upload a file to Google Cloud Storage (GCS).

    This function uploads a file to a specified GCS bucket. It uses the
    Google Cloud Storage client library to handle the upload process.

    Parameters
    ----------
    file_path : str
        The path to the file to be uploaded.
    bucket_name : str
        The name of the GCS bucket.
    destination_blob_name : str
        The name of the blob in the GCS bucket.

    Raises
    ------
    OSError
        If there is an error reading the file.
    GoogleCloudError
        If there is an error uploading the file to GCS.
    """
    # Create a GCS client.
    storage_client = storage.Client()

    # Get the bucket.
    bucket = storage_client.bucket(bucket_name)

    # Create a new blob.
    blob = bucket.blob(destination_blob_name)

    # Updload the file to GCS.
    try:
        blob.upload_from_filename(file_path)
    except (OSError, GoogleCloudError) as e:
        logging.error(
            f"Failed to upload file {file_path} to GCS bucket {bucket_name} "
            f"as {destination_blob_name}: {e}"
        )
        raise
