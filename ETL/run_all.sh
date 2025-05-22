#!/bin/bash

# Define all the data sources.
data_sources="AEMO_NEM \
AEMO_WEM \
AESO \
BCHYDRO \
CAMMESA \
CCEI \
CEN \
COES \
EIA \
EMI \
ENTSOE \
ESKOM \
HYDROQUEBEC \
IESO \
NBPOWER \
NESO \
NIGERIA \
ONS \
SONELGAZ \
TEPCO \
TSOC"

# Iterate over each data source and download the electricity time series data.
for source in $data_sources; do
    uv run /workspaces/electric-demand-data/ETL/download_electricity_data.py $source
done

# Download the population data.
uv run /workspaces/electric-demand-data/ETL/download_population_data.py

# Download the GDP data.
uv run /workspaces/electric-demand-data/ETL/download_gdp_data.py

# Download the weather data.
uv run /workspaces/electric-demand-data/ETL/download_weather_data.py

# Extract the temperature data.
uv run /workspaces/electric-demand-data/ETL/get_temperature_data.py
