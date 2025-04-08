import logging
import re
import time
from io import StringIO

import pandas
import requests
from entsoe import EntsoePandasClient
from entsoe.exceptions import NoMatchingDataError
from requests.exceptions import (
    ConnectionError,
    HTTPError,
    RequestException,
    SSLError,
    Timeout,
)


def fetch_data(
    url: str,
    content_type: str,
    output: str = "tabular",
    retries: int = 3,
    retry_delay: int = 5,
    csv_kwargs: dict = {},
    excel_kwargs: dict = {},
    verify_ssl: bool = True,
    response_params: dict = {},
    header_params: dict = {},
    json_keys: list[str] = [],
    query_event_target: str = "",
    query_params: dict[str, str] = {},
) -> pandas.DataFrame | str:
    """
    Fetch the data from the specified URL.

    Parameters
    ----------
    url : str
        The URL of the data source.
    content_type : str
        The type of the content to be fetched.
    output : str, optional
        The output format of the fetched data, by default "tabular"
    retries : int, optional
        The number of retries in case of connection errors, by default 3
    delay : int, optional
        The delay between retries in seconds, by default 5
    csv_kwargs : dict, optional
        The keyword arguments for reading CSV files, by default {}
    excel_kwargs : dict, optional
        The keyword arguments for reading Excel files, by default {}
    verify_ssl : bool, optional
        Verify the SSL certificate, by default True
    response_params : dict, optional
        The parameters for the response, by default {}
    json_keys : list[str], optional
        The keys to extract from the JSON response, by default []
    query_event_target : str, optional
        The event target for the query, by default ""
    query_params : dict[str, str], optional
        The parameters for the query, by default {}

    Returns
    -------
    pandas.DataFrame | str
        The fetched data as a DataFrame or a string.
    """

    for attempt in range(retries):
        try:
            if content_type == "csv":
                # Read the CSV file from the URL.
                return pandas.read_csv(url, **csv_kwargs)

            elif content_type == "excel":
                # Read the Excel file from the URL.
                return pandas.read_excel(url, **excel_kwargs)

            else:
                # Send the GET request.
                response = requests.get(
                    url,
                    timeout=10,
                    verify=verify_ssl,
                    headers=header_params,
                    params=response_params,
                )
                response.raise_for_status()

                if content_type == "text":
                    # Read the content of the response.
                    content = response.text

                    if output == "tabular":
                        # Return the content as a DataFrame.
                        return pandas.read_csv(StringIO(content), **csv_kwargs)
                    elif output == "text":
                        # Return the content as a string.
                        return content
                    else:
                        raise ValueError(f"Output format {output} is not supported.")

                elif content_type == "json":
                    # Read the content of the response.
                    content = response.json()

                    # Loop over the JSON keys and extract the content.
                    for json_key in json_keys:
                        content = content[json_key]

                    # Return the content as a DataFrame.
                    return pandas.DataFrame(content)

                elif content_type == "query":
                    # Read the content of the response.
                    html_content = response.text

                    # Extract the viewstate and eventvalidation parameters from the HTML content.
                    viewstate = re.findall(
                        r"id=\"__VIEWSTATE\" value=\"(.+)\"", html_content
                    )[0]
                    eventvalidation = re.findall(
                        r"id=\"__EVENTVALIDATION\" value=\"(.+)\"", html_content
                    )[0]

                    # Prepare the parameters for the POST request.
                    payload = {
                        "__VIEWSTATE": viewstate,
                        "__EVENTVALIDATION": eventvalidation,
                        "__EVENTTARGET": query_event_target,
                    }

                    # Add the additional parameters to the payload.
                    for key, value in query_params.items():
                        payload[key] = value

                    # Send the POST request.
                    response = requests.post(url, data=payload, timeout=10)
                    response.raise_for_status()

                    # Read the content of the response.
                    content = response.text

                    # Return the content as a DataFrame.
                    return pandas.read_csv(StringIO(content))

        except ConnectionError:
            logging.error(f"Connection error. Retrying ({attempt}/{retries})...")
            time.sleep(retry_delay)

        except Timeout:
            logging.error(f"Timeout error. Retrying ({attempt}/{retries})...")
            time.sleep(retry_delay)

        except SSLError:
            raise Exception("SSL error. Please verify the SSL certificate.")

        except HTTPError:
            raise Exception(
                f"HTTP error. The URL {url} is not valid or the server is not responding."
            )

        except RequestException:
            raise Exception(f"Request error. The URL {url} is not valid.")

    raise Exception(f"Failed to fetch data from {url} after {retries} retries.")


def fetch_entsoe_demand(
    api_key: str,
    iso_alpha_2_code: str,
    start_date_and_time: pandas.Timestamp,
    end_date_and_time: pandas.Timestamp,
    retries: int = 3,
    retry_delay: int = 5,
) -> pandas.Series:
    """
    Fetches the hourly electricity demand time series from ENTSO-E with retry logic.

    Parameters
    ----------
    api_key : str
        The API key for the ENTSO-E API
    iso_alpha_2_code : str
        The ISO Alpha-2 code of the country
    start_date_and_time : pandas.Timestamp
        The start date and time of the data retrieval
    end_date_and_time : pandas.Timestamp
        The end date and time of the data retrieval
    max_attempts : int, optional
        The maximum number of retry attempts (default is 3)
    retry_delay : int, optional
        The delay between retry attempts in seconds (default is 5)

    Returns
    -------
    pandas.Series
        The electricity demand time series in MW
    """

    # Define the ENTSO-E API client.
    client = EntsoePandasClient(api_key=api_key)

    try:
        for attempt in range(retries):
            try:
                return client.query_load(
                    iso_alpha_2_code, start=start_date_and_time, end=end_date_and_time
                )["Actual Load"]

            except ConnectionError:
                logging.error(f"Connection error. Retrying ({attempt}/{retries})...")
                time.sleep(retry_delay)

        raise ConnectionError(
            "Failed to connect to the ENTSO-E API after {retries} retries."
        )

    except NoMatchingDataError:
        # If the data is not available, skip to the next country.
        logging.warning(
            f"No data available for {iso_alpha_2_code} between {start_date_and_time} and {end_date_and_time}."
        )

        return pandas.Series()
