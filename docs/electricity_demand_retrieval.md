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

Each retrieval script in the `retrieval/` folder is designed to fetch electricity demand data from a specific data provider. While the structure may vary slightly, they generally follow this pattern:

- **Data Request Construction**: The script builds all data requests according to the data source availability.
- **URL Construction**: The script builds the appropriate API or web request URL.
- **Data Download**: It fetches the data using `util.fetcher` functions.
- **Data Processing**: The script applies necessary transformations, such as cleaning and time adjustments, using `util.time_series`.
- **Output Format**: The retrieved data is stored in CSV and Parquet formats.

## Logging

Logs are saved in a designated log directory.

## Contributing

Feel free to submit pull requests for additional data sources or improvements!

## License

This project is licensed under the AGPL-3.0 License.
