# Temperature time series generation

This document provides instructions on how to retrieve population density and weather data and generate a time series of the temperature.

## Population density data retrieval
To retrieve population density data from the Socioeconomic Data and Applications Center (SEDAC), run:

```uv run scripts/download_population_density_data.py```

This Python scritp downloads population density data at 30-sec resolution (about 1 km on the Equator) from SEDAC. It then extracts the population density of a given country or subdivision and coarsenes the data to the same resolution of the weather data (0.25°, or about 30 km on the Equator).

The script will store the population density data into `.nc` files within a folder called `data/population_density`.

## Weather data retrieval
To retrieve weather data from the Copernicus Climate Data Store (CDS), first ensure that you are registered on the website and have your API key stored in the `.cdsapirc` file in your home directory, as explained [here](https://cds.climate.copernicus.eu/how-to-api). Then run:

```uv run download_weather_data_from_Copernicus.py```

This Python scritp sets up the request according to the CDS API and downloads the selected weather variables (temperature) in the selected years and entities.

The script will store the weather data into `.nc` files within a folder called `data/weather`.

## Temperature data generation
To extract temperature time series according to population density, run:

```uv run get_temperature_data.py```

The script will extract time series of temperature based on the largest and three largest population density areas and store the results into `.csv` or `.parquet` files (specified inside the script) within a folder called `data/temperature`.
