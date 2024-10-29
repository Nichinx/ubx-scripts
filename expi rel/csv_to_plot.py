# -*- coding: utf-8 -*-
"""
Created on Sun Oct 20 08:06:58 2024

@author: nichm
"""

import numpy as np
import pandas as pd
from datetime import datetime as dt
from sqlalchemy import create_engine
from sqlalchemy.engine import Connection
import math
import matplotlib.pyplot as plt
from pyproj import Transformer

db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'admin123',
    'database': 'new_schema_3',
    'port': 3306
}

def create_db_connection():
    connection_string = f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    engine = create_engine(connection_string)
    return engine

def parse_line(line):
    split_line = line.strip().split('*')
    ts = split_line[1]
    
    sms = split_line[0]
    split_data = sms.split(':')
    logger_name = split_data[0]
    
    data_part = split_data[1]
    ublox_data = data_part.split(',')

    trans_ublox_data = pd.DataFrame([ublox_data], columns=["fix_type", "latitude", "longitude", "hacc", "vacc", "msl", "sat_num", "temp", "volt"])
    trans_ublox_data["ts"] = ts

    return logger_name, trans_ublox_data

transformer = Transformer.from_crs("epsg:4326", "epsg:32651", always_xy=True)
def convert_to_utm(lon, lat):
    easting, northing = transformer.transform(lon, lat)
    return easting, northing  # in meters

def euclidean_distance(easting, northing, ref_easting, ref_northing):
    distance_m = math.sqrt((easting - ref_easting) ** 2 + (northing - ref_northing) ** 2)
    return distance_m

# Function to get rover reference point
def get_rover_reference_point(rover_name):
    """Get the rover reference point (latitude, longitude)."""
    connection = create_db_connection()
    if connection:
        query = f"SELECT latitude, longitude FROM rover_reference_point WHERE rover_name = '{rover_name}';"
        result = pd.read_sql(query, connection)
        return result.iloc[0].tolist() if not result.empty else (None, None)
    return (None, None)

# Main function to fetch data and plot
def plot_gnss_data(logger_name):
    # Fetch data from the SQL table
    connection = create_db_connection()
    if connection:
        query = f"SELECT ts, latitude, longitude FROM gnss_{logger_name.lower()};"
        gnss_data = pd.read_sql(query, connection)
    
    # Get the rover reference point
    rover_name = logger_name
    ref_lat, ref_lon = get_rover_reference_point(rover_name)
    
    if ref_lat is None or ref_lon is None:
        print(f"No reference point found for rover: {rover_name}")
        return
    
    ref_easting, ref_northing = convert_to_utm(ref_lon, ref_lat)
    
    # Convert the lat/lon data to UTM and calculate distances
    gnss_data[['easting', 'northing']] = gnss_data.apply(lambda row: convert_to_utm(row['longitude'], row['latitude']), axis=1, result_type='expand')
    gnss_data['distance_m'] = gnss_data.apply(lambda row: euclidean_distance(row['easting'], row['northing'], ref_easting, ref_northing), axis=1)
    
    # # Plotting
    # plt.figure(figsize=(10, 6))
    # plt.scatter(gnss_data['easting'], gnss_data['northing'], c=gnss_data['distance_cm'], cmap='viridis', label='Distance from Reference Point (cm)')
    # plt.colorbar(label='Distance (cm)')
    # plt.title(f'GNSS Data for {logger_name}')
    # plt.xlabel('Easting (meters)')
    # plt.ylabel('Northing (meters)')
    # plt.grid()
    # plt.legend()
    # plt.show()
    
    # Plotting
    plt.figure(figsize=(12, 6))
    plt.plot(gnss_data['ts'], gnss_data['distance_m'], marker='o', linestyle='-')
    plt.title('Distance to Reference Point from UP Baseline')
    plt.xlabel('Timestamp')
    plt.ylabel('Distance (meters)')
    plt.xticks(rotation=45)  # Rotate timestamps for better visibility
    plt.grid()
    plt.tight_layout()
    plt.show()


# Path to your text file
file_path = "C:\\Users\\nichm\\Downloads\\UP_muhon_test.csv"  #Update this to your actual file path

dataframes = []
logger_names = set()
with open(file_path, 'r') as file:
    for line in file:
        logger_name, parsed_data = parse_line(line)
        if parsed_data is not None:
            dataframes.append(parsed_data)
            logger_names.add(logger_name)

# Concatenate all DataFrames into a single DataFrame
if dataframes:
    final_df = pd.concat(dataframes, ignore_index=True)
    print("Final DataFrame:")
    print(final_df)
    
    for logger_name in logger_names:
        plot_gnss_data(logger_name)
         
else:
    print("No valid data found to parse and plot.")
    
    
    
    
