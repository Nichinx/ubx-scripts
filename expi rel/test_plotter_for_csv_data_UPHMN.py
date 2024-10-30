# -*- coding: utf-8 -*-
"""
Created on Thu Oct 31 01:40:11 2024

@author: Nichi
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import math
from pyproj import Transformer

# Transformer for UTM conversion
transformer = Transformer.from_crs("epsg:4326", "epsg:32651", always_xy=True)

# Function to parse each line
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

# Convert latitude and longitude to UTM coordinates
def convert_to_utm(lon, lat):
    easting, northing = transformer.transform(lon, lat)
    return easting, northing  # in meters

# Calculate Euclidean distance
def euclidean_distance(easting, northing, ref_easting, ref_northing):
    return math.sqrt((easting - ref_easting) ** 2 + (northing - ref_northing) ** 2)

# Load reference point (latitude and longitude) for the rover manually
ref_lat, ref_lon = 15.490612200, 120.564817800  # Example reference point; replace with your actual values
ref_easting, ref_northing = convert_to_utm(ref_lon, ref_lat)

# Load the GNSS data from CSV instead of database
file_path = "C:\\Users\\Nichi\\Downloads\\UPMHN_2.csv"  # Update this to your actual file path

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
    gnss_data = pd.concat(dataframes, ignore_index=True)
    
    # Convert latitude and longitude columns to numeric
    gnss_data["latitude"] = pd.to_numeric(gnss_data["latitude"], errors='coerce')
    gnss_data["longitude"] = pd.to_numeric(gnss_data["longitude"], errors='coerce')
    gnss_data.dropna(subset=["latitude", "longitude"], inplace=True)
    
    # Convert lat/lon to UTM and calculate distances
    gnss_data[['easting', 'northing']] = gnss_data.apply(lambda row: convert_to_utm(row['longitude'], row['latitude']), axis=1, result_type='expand')
    gnss_data['distance_m'] = gnss_data.apply(lambda row: euclidean_distance(row['easting'], row['northing'], ref_easting, ref_northing), axis=1)
    
    gnss_data['ts'] = pd.to_datetime(gnss_data['ts'], format='%y%m%d%H%M%S')
    gnss_data['vacc'] = pd.to_numeric(gnss_data['vacc'], errors='coerce')
    
    # # Plot the GNSS data
    # plt.figure(figsize=(12, 6))
    # plt.plot(gnss_data['ts'], gnss_data['distance_m'], marker='o', linestyle='-')
    # plt.title('Distance to Reference Point from UP Baseline')
    # plt.xlabel('Timestamp')
    # plt.ylabel('Distance (meters)')
    # plt.xticks(rotation=45)  # Rotate timestamps for better visibility
    # plt.grid()
    # plt.tight_layout()
    # plt.show()
    
    
    
    
    
    # # Plotting both distance line plot and VACC scatter plot on the same figure
    # fig, ax1 = plt.subplots(figsize=(12, 6))

    # # Define VACC range for color bar scaling
    # vacc_min, vacc_max = gnss_data['vacc'].min(), gnss_data['vacc'].max()

    # # Line plot for Distance vs. Timestamp
    # ax1.plot(gnss_data['ts'], gnss_data['distance_m'], color='blue', marker='o', label='Distance (m)')
    # ax1.set_xlabel('Timestamp')
    # ax1.set_ylabel('Distance (meters)', color='blue')
    # ax1.tick_params(axis='y', labelcolor='blue')
    
    # # Scatter plot for VACC with color intensity
    # sc = ax1.scatter(gnss_data['ts'], gnss_data['distance_m'], c=gnss_data['vacc'], cmap='plasma', marker='o', vmin=vacc_min, vmax=vacc_max)
    # plt.colorbar(sc, ax=ax1, label='VACC (Vertical Accuracy)')  # Color bar for VACC intensity
    
    # # # Formatting x-axis for better visibility
    # # ax1.xaxis.set_major_locator(mdates.MinuteLocator(interval=1))
    # # ax1.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
    # # plt.xticks(rotation=45)
    
    # # Title and grid
    # plt.title('Distance and VACC Intensity over Time')
    # ax1.grid()
    # plt.tight_layout()
    # plt.show()
    
    
    
    
    
    # Plotting both distance line plot and VACC-colored markers on the same figure
    fig, ax1 = plt.subplots(figsize=(12, 6))
    
    # Define VACC range for color bar scaling
    vacc_min, vacc_max = gnss_data['vacc'].min(), gnss_data['vacc'].max()

    
    # Line plot for Distance vs. Timestamp without markers
    ax1.plot(gnss_data['ts'], gnss_data['distance_m'], color='blue', label='Distance (m)', zorder=1)
    
    # Scatter plot with VACC intensity-based color markers
    sc = ax1.scatter(gnss_data['ts'], gnss_data['distance_m'], c=gnss_data['vacc'], cmap='plasma', marker='o', vmin=vacc_min, vmax=vacc_max, zorder=2)
    plt.colorbar(sc, ax=ax1, label='VACC (Vertical Accuracy)')  # Color bar for VACC intensity
    
    # Label and tick formatting
    ax1.set_xlabel('Timestamp')
    ax1.set_ylabel('Distance (meters)', color='blue')
    ax1.tick_params(axis='y', labelcolor='blue')
    
    # Title and grid
    plt.title('Distance to Reference Point from UP Baseline, with VACC-Intensity Colored Markers over Time', fontweight='bold')
    ax1.grid()
    plt.tight_layout()
    plt.show()
    
    

else:
    print("No valid data found to parse and plot.")
