# -*- coding: utf-8 -*-
"""
License: AGPL-3.0.

Description:
    This module contains tests for the fetcher module.
"""

import urllib.error
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import requests
from entsoe.exceptions import NoMatchingDataError
from utils.fetcher import _read_aspx_params, fetch_data, fetch_entsoe_demand

# ------------------------
# _read_aspx_params Tests
# ------------------------


def test_read_aspx_params():
    mock_response = MagicMock()
    mock_response.text = """
    <input type="hidden" name="__VIEWSTATE" id="__VIEWSTATE" value="viewstate_value" />
    <input type="hidden" name="__EVENTVALIDATION" id="__EVENTVALIDATION" value="eventvalidation_value" />
    """
    post_data = {"foo": "bar"}
    updated = _read_aspx_params(mock_response, post_data.copy())
    assert updated["__VIEWSTATE"] == "viewstate_value"
    assert updated["__EVENTVALIDATION"] == "eventvalidation_value"
    assert updated["foo"] == "bar"


# ---------------------
# fetch_data Tests
# ---------------------


@patch("pandas.read_csv")
def test_fetch_data_csv(mock_read_csv):
    mock_read_csv.return_value = pd.DataFrame({"a": [1]})
    df = fetch_data("http://example.com/file.csv", "csv")
    assert isinstance(df, pd.DataFrame)


@patch("pandas.read_excel")
def test_fetch_data_excel(mock_read_excel):
    mock_read_excel.return_value = pd.DataFrame({"a": [1]})
    df = fetch_data("http://example.com/file.xlsx", "excel")
    assert isinstance(df, pd.DataFrame)


@patch("urllib.request.urlopen")
def test_fetch_data_html_urllib(mock_urlopen):
    mock_urlopen.return_value.read.return_value = b"<html>Content</html>"
    html = fetch_data(
        "http://example.com",
        "html",
        read_with="urllib.request",
        header_params={"User-Agent": "test"},
    )
    assert "<html>" in html


@patch("requests.get")
def test_fetch_data_html_requests_get_tabular(mock_get):
    mock_get.return_value.status_code = 200
    mock_get.return_value.text = "col1,col2\n1,2"
    mock_get.return_value.raise_for_status = lambda: None
    df = fetch_data("http://example.com", "html")
    assert isinstance(df, pd.DataFrame)


@patch("requests.get")
def test_fetch_data_html_requests_get_text(mock_get):
    mock_get.return_value.status_code = 200
    mock_get.return_value.text = "text content"
    mock_get.return_value.raise_for_status = lambda: None
    text = fetch_data("http://example.com", "html", read_as="text")
    assert text == "text content"


@patch("requests.post")
def test_fetch_data_html_requests_post_json(mock_post):
    mock_post.return_value.status_code = 200
    mock_post.return_value.raise_for_status = lambda: None
    mock_post.return_value.json = lambda: {"data": [{"a": 1}]}
    df = fetch_data(
        "http://example.com",
        "html",
        read_with="requests.post",
        read_as="json",
        json_keys=["data"],
    )
    assert isinstance(df, pd.DataFrame)


@patch("requests.get")
def test_fetch_data_html_requests_get_plain(mock_get):
    mock_get.return_value = requests.Response()
    mock_get.return_value.status_code = 200
    response = fetch_data("http://example.com", "html", read_as="plain")
    assert isinstance(response, requests.Response)


@patch("requests.get")
@patch("requests.post")
def test_fetch_data_html_requests_post_aspx(mock_post, mock_get):
    mock_get.return_value.text = """
        <input id="__VIEWSTATE" value="VS" />
        <input id="__EVENTVALIDATION" value="EV" />
    """
    mock_post.return_value.raise_for_status = lambda: None
    mock_post.return_value.text = "col1,col2\n1,2"
    df = fetch_data(
        "http://example.com",
        "html",
        read_with="requests.post",
        query_aspx_webpage=True,
        read_as="tabular",
    )
    assert isinstance(df, pd.DataFrame)


def test_fetch_data_invalid_type():
    with pytest.raises(ValueError):
        fetch_data("http://example.com", "unsupported")


def test_fetch_data_invalid_read_as():
    with patch("requests.get") as mock_get:
        mock_get.return_value.text = "text"
        mock_get.return_value.raise_for_status = lambda: None
        with pytest.raises(ValueError):
            fetch_data("http://example.com", "html", read_as="unknown")


def test_fetch_data_invalid_library():
    with pytest.raises(ValueError):
        fetch_data("http://example.com", "html", read_with="unknown_lib")


# --------------------------
# fetch_entsoe_demand Tests
# --------------------------


@patch("utils.fetcher.EntsoePandasClient")
def test_fetch_entsoe_demand_success(mock_client_class):
    mock_series = pd.Series(
        [1, 2], index=pd.date_range("2023-01-01", periods=2, freq="h")
    )
    mock_client = mock_client_class.return_value
    mock_client.query_load.return_value = pd.DataFrame(
        {"Actual Load": mock_series}
    )
    result = fetch_entsoe_demand(
        "dummy", "FR", pd.Timestamp("2023-01-01"), pd.Timestamp("2023-01-02")
    )
    assert isinstance(result, pd.Series)


@patch("utils.fetcher.EntsoePandasClient")
def test_fetch_entsoe_demand_connection_failure(mock_client_class):
    mock_client = mock_client_class.return_value
    mock_client.query_load.side_effect = ConnectionError("Connection failed")
    with pytest.raises(ConnectionError):
        fetch_entsoe_demand(
            "dummy",
            "FR",
            pd.Timestamp("2023-01-01"),
            pd.Timestamp("2023-01-02"),
            retries=2,
            retry_delay=0,
        )


@patch("utils.fetcher.EntsoePandasClient")
def test_fetch_entsoe_demand_no_data(mock_client_class):
    mock_client = mock_client_class.return_value
    mock_client.query_load.side_effect = NoMatchingDataError("No data")
    result = fetch_entsoe_demand(
        "dummy", "FR", pd.Timestamp("2023-01-01"), pd.Timestamp("2023-01-02")
    )
    assert isinstance(result, pd.Series) and result.empty


@patch("requests.get")
def test_fetch_data_requests_get_errors(mock_get):
    """Test fetch_data with different errors."""
    # Define the erors to test.
    errors = [
        requests.exceptions.ConnectionError(),
        requests.exceptions.Timeout(),
        requests.exceptions.HTTPError(),
        requests.exceptions.RequestException(),
        requests.exceptions.SSLError(),
    ]
    for error in errors:
        mock_get.side_effect = error
        with pytest.raises(Exception) as exc_info:
            fetch_data("http://example.com", "html", retries=1, retry_delay=0)

        with open("ETL/tests/test_fetcher.log", "a") as log_file:
            log_file.write(f"Error: {exc_info.value}\n")


@patch("urllib.request.urlopen")
def test_fetch_data_urlopen_errors(mock_urlopen):
    """Test fetch_data with different errors."""
    # Define the erors to test.
    errors = [
        urllib.error.HTTPError(url="", code=0, msg="", hdrs=None, fp=None),
        urllib.error.URLError(reason=""),
    ]
    for error in errors:
        mock_urlopen.side_effect = error
        with pytest.raises(Exception):
            fetch_data(
                "http://example.com",
                "html",
                retries=1,
                retry_delay=0,
                read_with="urllib.request",
            )
