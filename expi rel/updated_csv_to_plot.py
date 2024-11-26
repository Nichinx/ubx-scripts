# -*- coding: utf-8 -*-
"""
Created on Thu Oct 31 01:26:41 2024

@author: nichm
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import math
from pyproj import Transformer
import itertools
import seaborn as sns
import matplotlib.pyplot as plt


transformer = Transformer.from_crs("epsg:4326", "epsg:32651", always_xy=True)

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


def convert_to_utm(lon, lat):
    easting, northing = transformer.transform(lon, lat)
    return easting, northing  # in meters


def euclidean_distance(easting, northing, ref_easting, ref_northing):
    return math.sqrt((easting - ref_easting) ** 2 + (northing - ref_northing) ** 2)


# ref_lat, ref_lon = 15.490612200, 120.564817800  # Example reference point; replace with your actual values
ref_lat, ref_lon = 14.655230940, 121.059707000
ref_easting, ref_northing = convert_to_utm(ref_lon, ref_lat)

file_path = "C:\\Users\\nichm\\Documents\\GIT\\ubx-scripts\\expi rel\\data\\UPMHN_data_11072024\\UPMHN_1.csv"  # Update this to your actual file path

dataframes = []
logger_names = set()
# with open(file_path, 'r') as file:
#     for line in file:
#         logger_name, parsed_data = parse_line(line)
#         if parsed_data is not None:
#             dataframes.append(parsed_data)
#             # logger_names.add(logger_name)

with open(file_path, 'r') as file:
    # Skip the first two lines
    for line in itertools.islice(file, 2, None):
        logger_name, parsed_data = parse_line(line)
        if parsed_data is not None:
            dataframes.append(parsed_data)


if dataframes:
    gnss_data = pd.concat(dataframes, ignore_index=True)

    gnss_data['ts'] = pd.to_datetime(gnss_data['ts'], format='%y%m%d%H%M%S')
    gnss_data["latitude"] = pd.to_numeric(gnss_data["latitude"], errors='coerce')
    gnss_data["longitude"] = pd.to_numeric(gnss_data["longitude"], errors='coerce')
    gnss_data["msl"] = pd.to_numeric(gnss_data["msl"], errors='coerce')
    gnss_data['vacc'] = pd.to_numeric(gnss_data['vacc'], errors='coerce')
    
    gnss_data.dropna(subset=["latitude", "longitude"], inplace=True)
    gnss_data[['easting', 'northing']] = gnss_data.apply(lambda row: convert_to_utm(row['longitude'], row['latitude']), axis=1, result_type='expand')
    gnss_data['distance_m'] = gnss_data.apply(lambda row: euclidean_distance(row['easting'], row['northing'], ref_easting, ref_northing), axis=1)

    fig, ax1 = plt.subplots(figsize=(12, 6))
    ax1.plot(gnss_data['ts'], gnss_data['distance_m'], color='blue', label='Distance (m)', alpha=0.6, zorder=1)
    
    vacc_min, vacc_max = gnss_data['vacc'].min(), gnss_data['vacc'].max()
    sc = ax1.scatter(gnss_data['ts'], gnss_data['distance_m'], c=gnss_data['vacc'], cmap='plasma', marker='o', s=20, vmin=vacc_min, vmax=vacc_max, zorder=2)
    plt.colorbar(sc, ax=ax1, label='VACC (Vertical Accuracy, in meters)')
    
    ax2 = ax1.twinx() 
    ax2.plot(gnss_data['ts'], gnss_data['msl'], color='green', label='MSL (meters)', zorder=3, alpha=0.5)
    ax2.set_ylabel('Mean Sea Level (meters)', color='green')
    ax2.tick_params(axis='y', labelcolor='green')

    ax1.set_xlabel('Timestamp')
    ax1.set_ylabel('Distance (meters)', color='blue')
    ax1.tick_params(axis='y', labelcolor='blue')

    # plt.title('Distance to Reference Point from UP Baseline, with VACC-Intensity Colored Markers and MSL over Time', fontweight='bold')
    plt.title('Distance to Rover from Base, with VACC-Intensity Colored Markers and MSL over Time', fontweight='bold')
    ax1.grid()
    plt.tight_layout()
    plt.show()
    
    
    
    ##########  HEATMAP / HISTOGRAM
    expected_distance = 25  # in meters, as per your example

    # gnss_data['distance_m'] = gnss_data.apply(
    #     lambda row: euclidean_distance(row['easting'], row['northing'], ref_easting, ref_northing),
    #     axis=1
    # )
    gnss_data['percentage_error'] = ((abs(gnss_data['distance_m'] - expected_distance) / expected_distance) * 100)
    # # gnss_data['normalized_easting'] = gnss_data['easting'] - ref_easting
    # # gnss_data['normalized_northing'] = gnss_data['northing'] - ref_northing
    
    # # # Heatmap
    # # heatmap_data = gnss_data.pivot_table(
    # #     index='normalized_northing',
    # #     columns='normalized_easting',
    # #     values='percentage_error',
    # #     aggfunc='mean'  # Replace 'mean' with other functions like 'max' or 'min' if needed
    # # )
    # # plt.figure(figsize=(10, 8))
    # # sns.heatmap(heatmap_data, cmap='coolwarm', annot=False, cbar_kws={'label': 'Percentage Error (%)'})
    # # plt.title('Heatmap of Percentage Error')
    # # plt.show()
    
    # Scatter plot with a histogram on percentage_error
    plt.figure(figsize=(12, 6))
    plt.subplot(1, 2, 1)
    plt.scatter(gnss_data['easting'], gnss_data['northing'], c=gnss_data['percentage_error'], cmap='coolwarm', s=10)
    plt.colorbar(label='Percentage Error (%)')
    plt.xlabel('Easting')
    plt.ylabel('Northing')
    plt.title('Scatter Plot of Percentage Error')
    
    plt.subplot(1, 2, 2)
    sns.histplot(gnss_data['percentage_error'], bins=20, kde=True, color='blue')
    plt.xlabel('Percentage Error (%)')
    plt.ylabel('Frequency')
    plt.title('Histogram of Percentage Error')
    
    plt.tight_layout()
    plt.show()
    
    ########## 
    

    # expected_distance = 25  
    
    # gnss_data['distance_m'] = gnss_data.apply(
    #     lambda row: euclidean_distance(row['easting'], row['northing'], ref_easting, ref_northing),
    #     axis=1
    # )
    # gnss_data['percentage_error'] = ((abs(gnss_data['distance_m'] - expected_distance) / expected_distance) * 100)
    

    # heatmap_data = gnss_data.pivot_table(
    #     index='vacc',  
    #     columns='percentage_error',  
    #     values='percentage_error',  
    #     aggfunc='mean'  
    # )
    
    # plt.figure(figsize=(12, 8))
    # sns.heatmap(
    #     heatmap_data,
    #     cmap='coolwarm',
    #     annot=False,
    #     cbar_kws={'label': 'Percentage Error (%)'},
    # )
    
    # plt.title('Heatmap of Percentage Error vs Vertical Accuracy', fontsize=16)
    # plt.xlabel('Percentage Error (%)', fontsize=12)
    # plt.ylabel('Vertical Accuracy (m)', fontsize=12)
    # plt.xlim(0, gnss_data['percentage_error'].max())
    # plt.ylim(0, gnss_data['vacc'].max())
    # plt.show()
    
    #############
   
    def decimal_to_dms(decimal, is_latitude=True, precision=6):
        """
        Convert decimal degrees to degrees, minutes, and seconds (DMS) format with customizable precision.
        
        Args:
        - decimal: Decimal degree value (float).
        - is_latitude: True if converting latitude, False for longitude.
        - precision: Number of decimal places for seconds.
        
        Returns:
        - DMS string.
        """
        degrees = int(decimal)
        minutes = int(abs(decimal - degrees) * 60)
        seconds = (abs(decimal - degrees) * 60 - minutes) * 60
        direction = ('N' if decimal >= 0 else 'S') if is_latitude else ('E' if decimal >= 0 else 'W')
        return f"{abs(degrees)}° {minutes}' {seconds:.{precision}f}\" {direction}"

    
    def generate_dms_ticks(start, end, interval, is_latitude=True, precision=6):
        """
        Generate ticks in DMS format at a specified interval.
        
        Args:
        - start: Starting decimal degree value.
        - end: Ending decimal degree value.
        - interval: Interval in seconds (as decimal degree value).
        - is_latitude: True for latitude, False for longitude.
        - precision: Decimal places for seconds.
        
        Returns:
        - List of tick positions and labels.
        """
        ticks = np.arange(start, end, interval)
        labels = [decimal_to_dms(tick, is_latitude=is_latitude, precision=precision) for tick in ticks]
        return ticks, labels
    
    # Define the interval in decimal degrees for 0.001 seconds
    # 0.001 second corresponds to approximately 2.7778e-07 decimal degrees
    interval = 8.983e-09
    
    # Generate ticks for latitude and longitude
    lat_ticks, lat_labels = generate_dms_ticks(
        gnss_data['latitude'].min(),
        gnss_data['latitude'].max(),
        interval,
        is_latitude=True,
        precision=6
    )
    
    lon_ticks, lon_labels = generate_dms_ticks(
        gnss_data['longitude'].min(),
        gnss_data['longitude'].max(),
        interval,
        is_latitude=False,
        precision=6
    )
    
    # Create the scatter plot
    plt.figure(figsize=(12, 8))
    sc = plt.scatter(
        gnss_data['longitude'],  # Use decimal degrees for plotting
        gnss_data['latitude'],   # Use decimal degrees for plotting
        c=gnss_data['vacc'],     # Color by vertical accuracy
        cmap='viridis',
        s=20,
        alpha=0.8
    )
    
    # Add a color bar for VACC
    plt.colorbar(sc, label='Vertical Accuracy (m)')
    
    # Add labels and title
    plt.xlabel('Longitude (DMS)')
    plt.ylabel('Latitude (DMS)')
    plt.title('Scatter Plot of Latitude and Longitude with Vertical Accuracy')
    
    # Set the ticks and labels
    plt.xticks(ticks=lon_ticks, labels=lon_labels, rotation=45, fontsize=8)
    plt.yticks(ticks=lat_ticks, labels=lat_labels, fontsize=8)
    
    plt.tight_layout()
    plt.grid(True)
    plt.show()
        
    ###############

else:
    print("No valid data found to parse and plot.")
