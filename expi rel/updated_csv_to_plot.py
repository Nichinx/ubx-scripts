# -*- coding: utf-8 -*-
"""
Created on Thu Oct 31 01:26:41 2024

@author: nichm
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import math
from pyproj import Transformer
import itertools


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

file_path = "C:\\Users\\nichm\\Documents\\GIT\\ubx-scripts\\expi rel\\data\\UPMHN_data_11072024\\UPMHN_7.csv"  # Update this to your actual file path

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
    

else:
    print("No valid data found to parse and plot.")
