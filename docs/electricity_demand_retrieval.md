# Electricity demand data retrieval

This repository provides scripts to download and process electricity demand data from various sources. The main script, `download_electricity_data.py`, serves as the entry point for retrieving data, while individual retrieval scripts are located in the `retrieval/` folder, each handling data extraction from a specific source.

## Features

- Supports multiple electricity data sources including ENTSO-E, EIA, CCEI, and more.
- Downloads electricity demand data for specified countries or regions.
- Saves data in CSV and Parquet formats.
- Uses logging for tracking the retrieval process.

## Usage

### Running the Main Script

To download electricity data, run:

```bash
uv run download_electricity_data.py <data_source> [-c countryor_region_code] [-f code_file]
```

Arguments:

- `<data_source>`: The acronym of the data source (e.g., `ENTSOE`).
- `-c, --code`: (Optional) The country or region code (e.g., `US`, `US_CAL`).
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

Each retrieval script in the `retrieval/` folder is designed to fetch electricity demand data from a specific data source. The main functions in each script typically include:

- **Data Request Construction (`get_available_requests`)**: Builds all data requests based on the availability of the data source.
- **URL Construction (`get_url`)**: Generates the appropriate web request URL.
- **Data Download and Processing (`download_end_extract_data_for_request`)**: Fetches the data using `util.fetcher` functions and transforms it into a `pandas.Series`.

## Country and Region Names and Codes

For each retrieval script in the `retrieval/` folder, a corresponding YAML file must be created, listing all available countries or regions from the respective data source. The names and codes should adhere to the ISO 3166 standard. For country codes, please use alpha-2 codes. The regions are typically the principal subdivisions of a country (e.g., provinces or states). For non-standard subdivisions, please use a widely accepted name and code.

## Logging

Logs are saved in a designated log directory.

## Contributing

Feel free to submit pull requests for additional data sources or improvements!

## License

This project is licensed under the AGPL-3.0 License.
