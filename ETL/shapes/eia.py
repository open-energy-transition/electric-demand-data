#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
License: AGPL-3.0

Description:

    This script retrieves the shapes of the US balancing authorities from the website of the US Energy Information Administration (EIA).

    It then merges the balancing authorities belonging to the same region and saves the shapefile.

    Source: https://atlas.eia.gov/datasets/eia::balancing-authorities/about
"""

import os

import geopandas

# Define the URL of the shapefile of the balancing authorities.
url = "https://hub.arcgis.com/api/v3/datasets/09550598922b429ca9f06b9a067257bd_255/downloads/data?format=shp&spatialRefId=3857&where=1%3D1"

# Read the shapefile of the balancing authorities as a GeoDataFrame.
balancing_authorities = geopandas.read_file(url)

# Change the projection of the shapefile to EPSG 4326.
balancing_authorities = balancing_authorities.to_crs(epsg=4326)

# Merge the balancing authorities belonging to the same region.
regions = balancing_authorities.dissolve(by="EIAregion")

# Define the codes of the regions.
region_codes = {
    "California": "US_CAL",
    "Carolinas": "US_CAR",
    "Central": "US_CENT",
    "Florida": "US_FLA",
    "Mid-Atlantic": "US_MIDA",
    "Midwest": "US_MIDW",
    "New England": "US_NE",
    "New York": "US_NY",
    "Northwest": "US_NW",
    "Southeast": "US_SE",
    "Southwest": "US_SW",
    "Tennessee": "US_TEN",
    "Texas": "US_TEX",
}

# Add the codes to the regions shapefile.
for region_name in regions.index:
    regions.loc[region_name, "EIAcode"] = region_codes[region_name]

# Select the columns of interest.
regions = regions[["EIAcode", "geometry"]]

# Reset the index.
regions = regions.reset_index()

# Rename the columns.
regions = regions.rename(columns={"EIAregion": "name", "EIAcode": "code"})

# Save the regions shapefile into ETL/shapes/eia.
shapes_dir = os.path.join(os.path.dirname(__file__), "eia")
os.makedirs(shapes_dir, exist_ok=True)
regions.to_file(os.path.join(shapes_dir, "eia.shp"), driver="ESRI Shapefile")
