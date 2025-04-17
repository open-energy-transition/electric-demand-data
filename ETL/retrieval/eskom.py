# -*- coding: utf-8 -*-
"""
License: AGPL-3.0

Description:

    This script retrieves the electricity demand data the website of Eskom in South Africa.

    The data is retrieved by submitting a request to the Eskom website.

    The user then receives a link on the provided email address to download the data.

    Source: https://www.eskom.co.za/dataportal/data-request-form/
"""

import logging
import os

import pandas
import util.fetcher
import util.general


def get_available_requests() -> None:
    """
    Get the list of available requests to retrieve the electricity demand data from the Eskom website.
    """

    logging.info("The data is retrieved all at once.")
    return None


def get_url() -> str:
    """
    Get the URL of the electricity demand data from the Eskom website.

    Returns
    -------
    str
        The URL of the electricity demand data
    """

    # Return the URL of the electricity demand data.
    return "https://www.eskom.co.za/dataportal/cf-api/CF600011bdba174"


def send_request(
    start_date: pandas.Timestamp,
    end_date: pandas.Timestamp,
    first_name: str,
    last_name: str,
    email_address: str,
    organization: str,
) -> None:
    """
    Send a request to the Eskom website to retrieve the electricity demand data.

    It takes between 12 and 24 hours to receive the email with the link to download the data.

    Parameters
    ----------
    start_date : pandas.Timestamp
        The start date of the data to retrieve
    end_date : pandas.Timestamp
        The end date of the data to retrieve
    first_name : str
        The first name of the user
    last_name : str
        The last name of the user
    email_address : str
        The email address of the user
    organization : str
        The organization of the user
    """

    # Check if the lenght of the date range is less than 5 years.
    assert end_date - start_date <= pandas.Timedelta(years=5), (
        "The maximum time range is 5 years."
    )

    # Get the URL of the electricity demand data.
    url = get_url()

    # Define the form data to send.
    form_data = {
        "_cf_verify": "7ec21f805d",  # This is a verification code that needs to be updated.
        "_wp_http_referer": "/dataportal/data-request-form/",
        "_cf_frm_id": "CF600011bdba174",
        "_cf_frm_ct": "1",
        "cfajax": "CF600011bdba174",
        "_cf_cr_pst": "1656",
        "twitter": "",
        "fld_8510825": first_name,
        "fld_9768035": last_name,
        "fld_2337893": email_address,
        "fld_7053797": email_address,
        "fld_2546748": organization,
        "fld_5416624": "I'm an analyst",
        "fld_2659067": "I accept",
        "fld_6891510": start_date.strftime("%Y-%m-%d"),
        "fld_8152082": end_date.strftime("%Y-%m-%d"),
        "fld_3439594": "use_selection",
        "fld_672698": "use_selection",
        "fld_9087658[opt1954245]": "RSA Contracted Demand",  # New param
        "fld_2127424": "use_selection",
        "fld_3687264": "use_selection",
        "fld_6863828": "use_selection",
        "fld_9590435": "use_selection",
        "fld_5458760": "click",
        "instance": "1",
        "request": "https://www.eskom.co.za/dataportal/cf-api/CF600011bdba174",
        "formId": "CF600011bdba174",
        "postDisable": "0",
        "target": "#caldera_notices_1",
        "loadClass": "cf_processing",
        "loadElement": "_parent",
        "hiderows": "true",
        "action": "cf_process_ajax_submit",
        "template": "#cfajax_CF600011bdba174-tmpl",
    }

    # Send the request to the Eskom website.
    __ = util.fetcher.fetch_data(
        url,
        "text",
        output_content_type="text",
        request_type="post",
        post_data_params=form_data,
    )

    return None


def download_and_extract_data() -> pandas.Series:
    """
    Extract the electricity demand data retrieved from the Eskom website.

    This function assumes that the data has been downloaded and is available in the specified folder.

    Returns
    -------
    electricity_demand_time_series : pandas.Series
        The electricity demand time series in MW
    """

    # Get the data folder.
    data_directory = util.general.read_folders_structure()["data_folder"]

    # Get the paths of the downloaded files. Each file starts with "ESK".
    downloaded_file_paths = [
        os.path.join(data_directory, file)
        for file in os.listdir(data_directory)
        if file.startswith("ESK")
    ]

    # Load the data from the downloaded files into a pandas DataFrame.
    dataset = pandas.concat(
        [pandas.read_csv(file_path) for file_path in downloaded_file_paths]
    )

    # Extract the electricity demand time series.
    electricity_demand_time_series = pandas.Series(
        dataset["RSA Contracted Demand"].values,
        index=pandas.to_datetime(
            dataset["Date Time Hour Beginning"], format="%Y-%m-%d %H:%M:%S %p"
        ),
    )

    # Add one hour to the index because the electricity demand seems to be provided at the beginning of the hour.
    electricity_demand_time_series.index = (
        electricity_demand_time_series.index + pandas.Timedelta(hours=1)
    )

    # Add the timezone information to the index.
    electricity_demand_time_series.index = (
        electricity_demand_time_series.index.tz_localize("Africa/Johannesburg")
    )

    return electricity_demand_time_series
