# Source: https://cds.climate.copernicus.eu/how-to-api

import logging
import os

import cdsapi
import pandas
import pytz
import util.general
import util.geospatial
import xarray


def get_request(
    ERA5_variable: str,
    year: int,
    region_bounds: list[float] | None = None,
    extra_time_steps: pandas.DatetimeIndex = None,
    uneven_utc_offset: bool = False,
) -> dict[str, str | list[str] | list[float]]:
    """
    Get the request for the ERA5 data from the Copernicus Climate Data Store (CDS).

    Parameters
    ----------
    ERA5_variable : str
        The ERA5 variable of interest
    year : int
        The year of the data retrieval
    region_bounds : list of float, optional
        The lateral bounds of the region of interest (West, South, East, North)
    extra_time_steps : pandas.DatetimeIndex, optional
        The extra time steps to consider
    uneven_utc_offset : bool, optional
        Whether the UTC offset is uneven

    Returns
    -------
    request : dict of str or list of str or list of float
        The request for the ERA5 data
    """

    # Initialize the request with the common parameters.
    request: dict[str, str | list[str] | list[float]] = {
        "product_type": ["reanalysis"],
        "variable": [ERA5_variable],
        "data_format": "netcdf",
        "download_format": "unarchived",
    }

    # Add the region bounds if they are provided.
    if region_bounds is not None:
        request["area"] = [
            region_bounds[3],
            region_bounds[0],
            region_bounds[1],
            region_bounds[2],
        ]  # North, West, South, East

    # Add the time steps of the year of interest or the extra time steps.
    if extra_time_steps is None:
        request["year"] = [str(year)]
        request["month"] = [f"{mm:02d}" for mm in range(1, 13)]
        request["day"] = [f"{dd:02d}" for dd in range(1, 32)]
        request["time"] = [f"{tt:02d}:00" for tt in range(24)]
    else:
        request["year"] = [str(extra_time_steps[0].year)]
        request["month"] = [f"{extra_time_steps[0].month:02d}"]
        request["day"] = [f"{extra_time_steps[0].day:02d}"]
        if uneven_utc_offset:
            # Add an additional time step to consider the uneven UTC offset.
            # If the additional time steps are in the following year and are the first time steps of the year, add an additional time step at the end of the time steps.
            if extra_time_steps.hour[0] == 0:
                extra_time_steps = extra_time_steps.union(
                    pandas.DatetimeIndex(
                        [extra_time_steps[-1] + pandas.Timedelta(hours=1)]
                    )
                )
            # If the additional time steps are in the previous year and are the last time steps of the year, add an additional time step at the beginning of the time steps.
            elif extra_time_steps.hour[-1] == 23:
                extra_time_steps = extra_time_steps.union(
                    pandas.DatetimeIndex(
                        [extra_time_steps[0] - pandas.Timedelta(hours=1)]
                    )
                )
        request["time"] = [f"{tt:02d}:00" for tt in extra_time_steps.hour]

    return request


def merge_temporary_temperature_datasets(
    file_path_1: str,
    file_path_2: str,
    file_path_final: str,
    time_steps: pandas.DatetimeIndex,
    uneven_utc_offset: bool,
) -> None:
    """
    Merge two temporary temperature datasets.

    Parameters
    ----------
    file_path_1 : str
        The file path of the first temperature dataset
    file_path_2 : str
        The file path of the second temperature dataset
    file_path_final : str
        The file path of the final temperature dataset
    time_steps : pandas.DatetimeIndex
        The time steps of the temperature to retain
    uneven_utc_offset : bool
        Whether the UTC offset is uneven
    """

    # Load the two temperature datasets.
    temperature_data_1 = xarray.open_dataarray(file_path_1)
    temperature_data_2 = xarray.open_dataarray(file_path_2)

    # Merge the two temperature datasets.
    temperature_data = xarray.concat(
        [temperature_data_1, temperature_data_2], dim="valid_time"
    )
    temperature_data = temperature_data.sortby("valid_time")

    if uneven_utc_offset:
        # Interpolate the temperature data, which is on whole hours, to the time steps in the local time zone, which are not on whole hours.
        temperature_data = temperature_data.interp(
            valid_time=time_steps, method="linear"
        )
    else:
        # Keep only the time steps of the year of interest in the local time zone.
        temperature_data = temperature_data.sel(valid_time=time_steps)

    # Save the final temperature dataset.
    temperature_data.to_netcdf(file_path_final)

    # Remove the temporary temperature datasets.
    os.remove(file_path_1)
    os.remove(file_path_2)


def download_ERA5_data_from_Copernicus(
    year: int,
    ERA5_variable: str,
    file_path: str,
    region_bounds: list[float] | None = None,
    local_time_zone: pytz.timezone = None,
) -> None:
    """
    Download the ERA5 data from the Copernicus Climate Data Store (CDS).

    Parameters
    ----------
    year : int
        The year of the data retrieval
    ERA5_variable : str
        The ERA5 variable of interest
    file_path : str
        The full file path to store the ERA5 data
    region_bounds : list of float, optional
        The lateral bounds of the region of interest (West, South, East, North)
    local_time_zone : pytz.timezone, optional
        The time zone of the region of interest
    """

    # Create a new CDS API client.
    client = cdsapi.Client()

    # Define the dataset.
    dataset = "reanalysis-era5-single-levels"

    if local_time_zone is None:
        # Define the request.
        request = get_request(ERA5_variable, year, region_bounds)
        client.retrieve(dataset, request, file_path)

    else:
        # Get all the time steps of the year in the time zone of the region and convert them to UTC.
        time_steps = (
            (
                pandas.date_range(
                    start=str(year), end=str(year + 1), freq="h", tz=local_time_zone
                )[:-1]
            )
            .tz_convert("UTC")
            .tz_convert(None)
        )

        # Find the time steps that are not in the year of interest (UTC) when considering the time zone of the region.
        extra_time_steps = time_steps[time_steps.year != year]

        # Get the time difference to check if the time zone of the region has an uneven UTC offset.
        time_difference = (
            pandas.Timestamp(str(year), tz=local_time_zone).utcoffset().total_seconds()
            / 3600
        )

        # If the time difference (in hours) is not an integer number of hours, the UTC offset is uneven.
        uneven_utc_offset = time_difference % 1 != 0

        if len(extra_time_steps) > 0:
            # Define the temporary file paths.
            temporaty_file_path_1 = file_path.replace(".nc", "__temporary_1.nc")
            temporaty_file_path_2 = file_path.replace(".nc", "__temporary_2.nc")

            # Define two requests: one for the year of interest and one for the extra time steps.
            request_1 = get_request(
                ERA5_variable,
                year,
                region_bounds,
                extra_time_steps=extra_time_steps,
                uneven_utc_offset=uneven_utc_offset,
            )
            request_2 = get_request(ERA5_variable, year, region_bounds)

            # Download the two temperature datasets.
            client.retrieve(dataset, request_1, temporaty_file_path_1)
            client.retrieve(dataset, request_2, temporaty_file_path_2)

            # Merge the two temperature datasets.
            merge_temporary_temperature_datasets(
                temporaty_file_path_1,
                temporaty_file_path_2,
                file_path,
                time_steps,
                uneven_utc_offset,
            )

        else:
            # Define the request.
            request = get_request(ERA5_variable, year, region_bounds)
            client.retrieve(dataset, request, file_path)


def run_weather_data_retrieval() -> None:
    """
    Run the weather data retrieval from the Copernicus Climate Data Store (CDS).
    """

    # Set up the logging configuration.
    log_files_directory = util.general.read_folders_structure()["log_files_folder"]
    os.makedirs(log_files_directory, exist_ok=True)
    log_file_name = "weather_data_from_Copernicus.log"
    logging.basicConfig(
        filename=os.path.join(log_files_directory, log_file_name),
        level=logging.INFO,
        filemode="w",
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    # Create a directory to store the weather data.
    result_directory = util.general.read_folders_structure()["weather_folder"]
    os.makedirs(result_directory, exist_ok=True)

    # Read the codes of the regions of interest.
    settings_directory = util.general.read_folders_structure()["settings_folder"]
    region_codes = util.general.read_codes_from_file(
        os.path.join(settings_directory, "gegis__all_countries.yaml")
    )

    # Define the ERA5 variables to download.
    era5_variables = ["2m_temperature"]

    # Define the years of the data retrieval.
    start_year = 2015
    end_year = 2015
    years = range(start_year, end_year + 1)

    # Loop over the years.
    for year in years:
        logging.info(f"Year {year}.")

        # Loop over the regions of interest.
        for region_code in region_codes:
            logging.info(f"Retrieving data for {region_code}.")

            # Loop over the ERA5 variables.
            for era5_variable in era5_variables:
                logging.info(f"Retrieving {era5_variable} data...")

                # Define the full file path of the ERA5 data.
                era5_data_file_path = os.path.join(
                    result_directory, f"{era5_variable}_{region_code}_{year}.nc"
                )

                # Check if the file does not exist.
                if not os.path.exists(era5_data_file_path):
                    # Get the region of interest.
                    region_shape = util.geospatial.get_geopandas_region(region_code)

                    # Get the lateral bounds of the region of interest.
                    region_bounds = util.geospatial.get_region_bounds(
                        region_shape
                    )  # West, South, East, North

                    # Get the time zone of the region.
                    region_time_zone = util.general.get_time_zone(region_code)

                    # Download the ERA5 data from the Copernicus Climate Data Store (CDS).
                    download_ERA5_data_from_Copernicus(
                        year,
                        era5_variable,
                        era5_data_file_path,
                        region_bounds=region_bounds,
                        local_time_zone=region_time_zone,
                    )

                    logging.info(f"Downloaded {era5_variable} data.")


if __name__ == "__main__":
    run_weather_data_retrieval()
