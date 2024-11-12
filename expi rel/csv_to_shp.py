# -*- coding: utf-8 -*-
"""
Created on Tue Nov 12 08:57:48 2024

@author: nichm
"""

import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

# Parse the CSV file line by line to extract latitude and longitude
lat_lon_data = []
with open('C:\\Users\\nichm\\Downloads\\UPMHN\\BASELINE.csv', 'r') as file:
    for line in file:
        split_line = line.strip().split('*')
        sms = split_line[0]
        
        # Parse logger name and data part
        split_data = sms.split(':')
        data_part = split_data[1]
        
        # Extract ublox data
        ublox_data = data_part.split(',')
        
        # Get latitude and longitude
        lat = float(ublox_data[1])
        lon = float(ublox_data[2])
        
        # Append to the list
        lat_lon_data.append({'lat': lat, 'lon': lon})

# Convert the list to a DataFrame
df = pd.DataFrame(lat_lon_data)

# Create a GeoDataFrame using the lat and lon columns
gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df['lon'], df['lat']))

# Set the coordinate reference system (CRS) to WGS84 (EPSG:4326)
gdf.set_crs("EPSG:4326", inplace=True)

# Export to a shapefile
gdf.to_file("baseline.shp", driver="ESRI Shapefile")

print("Shapefile has been created successfully.")
