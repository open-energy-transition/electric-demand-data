# -*- coding: utf-8 -*-
"""
License: AGPL-3.0.

Description:

    This module provides functions to upload files to Google Cloud
    Storage (GCS) and Zenodo.
"""

import json
import logging
import os

import pandas
import requests
from dotenv import load_dotenv
from google.cloud import storage
from google.cloud.exceptions import GoogleCloudError

import utils.directories


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


def upload_to_zenodo(
    file_path: str,
    zenodo_url: str,
) -> None:
    """
    Upload a file to Zenodo.

    This function uploads a file to Zenodo using the Zenodo API. It
    requires an access token for authentication.

    Parameters
    ----------
    file_path : str
        The path to the file to be uploaded.

    Raises
    ------
    Exception
        If there is an error uploading the file to Zenodo or if the
        response from Zenodo is not successful.
    """
    # Get the root directory of the project.
    root_directory = utils.directories.read_folders_structure()["root_folder"]

    # Load the environment variables.
    load_dotenv(dotenv_path=os.path.join(root_directory, ".env"))

    # Get the Zenodo access token from environment variables.
    access_token = os.getenv("ZENODO_ACCESS_TOKEN")

    # Define the deposition metadata for the dataset.
    data = {
        "metadata": {
            "title": "Global Electricity Demand Dataset",
            "upload_type": "dataset",
            "publication_date": pandas.Timestamp.now().strftime("%Y-%m-%d"),
            "description": (
                "This dataset contains synthetic electricity demand data at "
                "hourly resolution for various countries. The data is "
                "generated using machine learning models trained on "
                "historical demand data, weather data, and socioeconomic "
                "indicators."
            ),
            "creators": [
                {
                    "name": "Antonini, Enrico G. A.",
                    "affiliation": "Open Energy Transition",
                    "orcid": "0000-0002-5573-0954",
                },
                {
                    "name": "Vamsi Priya, Goli",
                    "affiliation": "Open Energy Transition",
                },
                {
                    "name": "Steijn, Kevin",
                    "affiliation": "Open Energy Transition",
                },
            ],
            "access_right": "open",
            "license": "agpl-1.0-or-later",
            "keywords": [
                "electricity",
                "demand",
                "synthetic data",
                "machine learning",
            ],
            "related_identifiers": [
                {
                    "relation": "isDerivedFrom",
                    "identifier": (
                        "https://github.com/open-energy-transition/demandcast"
                    ),
                    "resource_type": "software",
                }
            ],
        }
    }

    # Create a new deposition in Zenodo.
    response = requests.post(
        "https://zenodo.org/api/deposit/depositions",
        params={"access_token": access_token},
        data=json.dumps(data),
    )

    # Extract the bucket URL and deposition ID from the response.
    bucket_url = response.json()["links"]["bucket"]
    deposition_id = response.json()["id"]

    # Upload the file to Zenodo.
    with open(file_path, "rb") as file_to_upload:
        response = requests.put(
            f"{bucket_url}/{os.path.basename(file_path)}",
            data=file_to_upload,
            params={"access_token": access_token},
        )

        if response.status_code != 201:
            logging.error(
                f"Failed to upload file {file_path} to Zenodo: {response.text}"
            )
            raise Exception(f"Zenodo upload failed: {response.text}")

    # Publish the deposition in Zenodo.
    response = requests.post(
        (
            "https://zenodo.org/api/deposit/depositions/"
            f"{deposition_id}/actions/publish"
        ),
        params={"access_token": access_token},
    )
