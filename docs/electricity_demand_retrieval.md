# Electricity demand data retrieval

This document provides instructions on how to retrieve electricity demand data for European countries

## Data retrieval from ENTSO-E
To retrieve electricity demand data from the ENTSO-E platform, first ensure that you are registered on the platform and have your API key stored in the `.env` file. Then run the following command:

```python download_electricity_demand_data_from_ENTSO-E.py```

This Python scritp downloads the electricity demand for the countries specified in `settings/gegis__countries_on_entsoe_platform.txt` and for the years specified in the script itself. Note that data is typically available from 2015 for all European countries, with exceptions:
- United Kingdom: Available until 2021.
- Bosnia and Herzegovina: Available from 2017.
- Cyprus: Available from 2016 to 2022.
- Iceland: Not available.

The script will store time series of electricity demand into `.csv` or `.parquet` files (specified inside the script) within a folder called `data__electricity_demand`.

## Data retrieval for Cyprus
To retrieve electricity generation data of Cyprus (which could be considered as a proxy for electricity demand), run the following command:

```python download_electricity_generation_data_of_Cyprus.py```

The script will store time series of electricity generation into `.csv` or `.parquet` files (specified inside the script) within a folder called `data__electricity_generation`.
