#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
License: AGPL-3.0

Description:

    This script generates the shape of the four major subsystems of the Brazilian electricity sector.

    Source: https://pt.wikipedia.org/wiki/Sistema_Interligado_Nacional
"""

import os

import cartopy.io.shapereader
import geopandas
import pandas

# Define the codes of the Brazilian states and their corresponding regions.
codes_of_brazilian_regions = {
    "BR-AC": "BR_N",
    "BR-AP": "BR_N",
    "BR-AM": "BR_N",
    "BR-PA": "BR_N",
    "BR-RO": "BR_N",
    "BR-RR": "BR_N",
    "BR-TO": "BR_N",
    "BR-AL": "BR_NE",
    "BR-BA": "BR_NE",
    "BR-CE": "BR_NE",
    "BR-MA": "BR_NE",
    "BR-PB": "BR_NE",
    "BR-PI": "BR_NE",
    "BR-PE": "BR_NE",
    "BR-RN": "BR_NE",
    "BR-SE": "BR_NE",
    "BR-ES": "BR_SE",
    "BR-MG": "BR_SE",
    "BR-RJ": "BR_SE",
    "BR-SP": "BR_SE",
    "BR-GO": "BR_SE",
    "BR-MT": "BR_SE",
    "BR-MS": "BR_SE",
    "BR-DF": "BR_SE",
    "BR-PR": "BR_S",
    "BR-SC": "BR_S",
    "BR-RS": "BR_S",
}

# Define the names of the Brazilian regions.
names_of_brazilian_regions = {
    "BR_N": "North",
    "BR_NE": "North-East",
    "BR_SE": "South-East",
    "BR_S": "South",
}

# Load the shapefile containing the shapes of the regions from the Natural Earth database.
region_shapes = cartopy.io.shapereader.natural_earth(
    resolution="50m", category="cultural", name="admin_1_states_provinces"
)

# Define a reader for the shapefile.
reader = cartopy.io.shapereader.Reader(region_shapes)

# Read the shapefile of all Brazilian regions.
region_shapes = [ii for ii in list(reader.records()) if ii.attributes["iso_a2"] == "BR"]

# Create a DataFrame from the shapes of the regions.
regions = pandas.DataFrame(columns=["name", "code", "parent", "geometry"])
for region in region_shapes:
    region_shape = pandas.Series(
        {
            "name": region.attributes["name"],
            "code": region.attributes["iso_3166_2"],
            "parent": codes_of_brazilian_regions[region.attributes["iso_3166_2"]],
            "geometry": region.geometry,
        }
    )
    regions = pandas.concat([regions, region_shape.to_frame().T], ignore_index=True)

# Add the coordinate reference system to the GeoDataFrame.
regions = geopandas.GeoDataFrame(regions, geometry="geometry", crs="EPSG:4326")

# Merge the states belonging to the same region.
regions = regions.dissolve(by="parent")

# Reset the index of the GeoDataFrame.
regions = regions.reset_index()

# Drop the columns that are not needed.
regions = regions[["name", "parent", "geometry"]]

# Rename the columns of the GeoDataFrame.
regions = regions.rename(columns={"parent": "code"})

# Add the names of the regions to the GeoDataFrame.
for region_code in regions["code"]:
    regions.loc[regions["code"] == region_code, "name"] = names_of_brazilian_regions[
        region_code
    ]

# Save the shapes of the region to a shapefile.
shapes_dir = os.path.join(os.path.dirname(__file__), "ons")
os.makedirs(shapes_dir, exist_ok=True)
regions.to_file(os.path.join(shapes_dir, "ons.shp"), driver="ESRI Shapefile")
