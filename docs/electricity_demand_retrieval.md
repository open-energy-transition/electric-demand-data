# Electricity demand data retrieval

This repository provides scripts to download and process electricity demand data from various sources. The main script, `download_electricity_data.py`, serves as the entry point for retrieving data, while individual retrieval scripts are located in the `retrieval/` folder, each handling data extraction from a specific provider.

## Features

- Supports multiple electricity data sources including ENTSO-E, EIA, CCEI, and more.
- Downloads electricity demand data for specified countries or regions.
- Saves data in CSV and Parquet formats.
- Uses logging for tracking the retrieval process.

## Usage

### Running the Main Script

To download electricity data, run:

```bash
uv run download_electricity_data.py <data_source> [-c country_code] [-f code_file]
```

Arguments:

- `<data_source>`: The acronym of the data source (e.g., `ENTSOE`).
- `-c, --code`: (Optional) The country or region code (e.g., `US`, `FR`).
- `-f, --file`: (Optional) YAML file containing multiple codes.

### Example

Download electricity data for France from ENTSO-E:

```bash
uv run download_electricity_data.py ENTSOE -c FR
```

## Structure

```
.
├── retrieval/
│   ├── ENTSOE.py         # Retrieves data from ENTSO-E
│   ├── ENTSOE.yaml       # List of country names and codes available on ENTSO-E
│   ├── CCEI.py           # Retrieves data from CCEI
│   ├── CCEI.yaml         # List of region names and codes available on CCEI
│   ├── EIA.py            # Retrieves data from EIA
│   ├── EIA.yaml          # List of region names and codes available on EIA
│   ├── ...               # Other data sources
├── util/
│   ├── fetcher.py        # Functions to fetch online content
│   ├── general.py        # General utility functions
│   ├── time_series.py    # Time series processing
├── download_electricity_data.py  # Main script
├── .env                  # API keys (not included in repo)
```

## Retrieval Scripts

Each retrieval script in the `retrieval/` folder is designed to fetch electricity demand data from a specific data provider. The functions cointained in each retrieval script generally are:

- **Data Request Construction (`get_available_requests`)**: The function builds all data requests according to the data source availability.
- **URL Construction (`get_url`)**: The function builds the appropriate API or web request URL.
- **Data Download and Processing (`download_end_extract_data_for_request`)**: The function fetches the data using `util.fetcher` functions and trasnforms the data into a `pandas.Series`.

## Logging

Logs are saved in a designated log directory.

## Contributing

Feel free to submit pull requests for additional data sources or improvements!

## License

This project is licensed under the AGPL-3.0 License.
