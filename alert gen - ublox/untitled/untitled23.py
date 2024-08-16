# -*- coding: utf-8 -*-
"""
Created on Fri Aug  9 19:02:26 2024

@author: nichm
"""

import math
import pandas as pd
import numpy as np
import time
from datetime import datetime, time, timedelta
from pyproj import Proj, transform


df = pd.read_csv("C:\\Users\\nichm\\Downloads\\tes.csv")
df = df.sort_values(by='ts')


# Define the projection for WGS84 (latitude and longitude)
wgs84 = Proj(proj='latlong', datum='WGS84')

# Define the projection for UTM Zone 51N
utm_zone_51 = Proj(proj='utm', zone=51, datum='WGS84')


def convert_to_utm(lat, lon):
    easting, northing = transform(wgs84, utm_zone_51, lon, lat)
    return easting, northing #in meters

df[['latitude', 'longitude']] = df.apply(
    lambda row: convert_to_utm(row['latitude'], row['longitude']), 
    axis=1, 
    result_type='expand'
)

def euclidean_distance(x1, y1, x2, y2):
    distance = math.sqrt((x2 - x1)**2 + (y2 - y1)**2)
    distance_cm = distance * 100  # Convert to centimeters
    return distance_cm


def resample_df(df):
    df['ts'] = pd.to_datetime(df['ts'], unit='s')
    df = df.set_index('ts').resample('10min').first().reset_index()
    return df

# def euclidean_distance(lat1, lon1, lat2, lon2):
#     lat1_rad = math.radians(lat1)
#     lon1_rad = math.radians(lon1)
#     lat2_rad = math.radians(lat2)
#     lon2_rad = math.radians(lon2)

#     R = 6371000  # Radius of the Earth in meters

#     delta_lat = lat2_rad - lat1_rad
#     delta_lon = lon2_rad - lon1_rad

#     distance = math.sqrt((delta_lat * R)**2 + (delta_lon * R * math.cos((lat1_rad + lat2_rad) / 2))**2)
#     distance_cm = distance * 100

#     return distance_cm


def prepare_and_apply_sanity_filters(df, hacc, vacc):
    df['msl'] = np.round(df['msl'], 3)
    df = df[(df['fix_type'] == 2) & (df['sat_num'] > 28)]

    if df.empty:
        return df

    df = df[(df['hacc'] == hacc) & (df['vacc'] <= vacc)]
    df = df.reset_index(drop=True).sort_values(by='ts', ascending=True, ignore_index=True)

    return df

def outlier_filter_for_latlon_with_msl(df):
    df = df.copy()
    dfmean = df[['latitude', 'longitude', 'msl','distance_cm']].rolling(window=6, min_periods=1).mean()
    dfsd = df[['latitude', 'longitude', 'msl','distance_cm']].rolling(window=6, min_periods=1).std()

    dfulimits = dfmean + (2 * dfsd)  # 1 std
    dfllimits = dfmean - (2 * dfsd)

    df['latitude'] = df['latitude'].where((df['latitude'] <= dfulimits['latitude']) & (df['latitude'] >= dfllimits['latitude']), np.nan)
    df['longitude'] = df['longitude'].where((df['longitude'] <= dfulimits['longitude']) & (df['longitude'] >= dfllimits['longitude']), np.nan)
    df['msl'] = df['msl'].where((df['msl'] <= dfulimits['msl']) & (df['msl'] >= dfllimits['msl']), np.nan)
    df['distance_cm'] = df['distance_cm'].where((df['distance_cm'] <= dfulimits['distance_cm']) & (df['distance_cm'] >= dfllimits['distance_cm']), np.nan)

    df = df.dropna(subset=['latitude', 'longitude', 'msl','distance_cm'])

    return df


# Fixed coordinates - TES
fixed_lat = 14.6519327
fixed_lon = 121.0584508
fixed_lat, fixed_lon = convert_to_utm(fixed_lat, fixed_lon)



# Calculate distances
df['distance_cm'] = df.apply(lambda row: euclidean_distance(row['latitude'], row['longitude'], fixed_lat, fixed_lon), axis=1)
print('df len = ', len(df))


# Convert 'ts' column to datetime
df['ts'] = pd.to_datetime(df['ts'])
df = resample_df(df).fillna(method='ffill')


num_timestamps_ahead = 6
timestamp_freq = '10T'  # Frequency of timestamps in original data

# Get the starting timestamp and generate new timestamps
start_ts = df['ts'].min() - pd.Timedelta(timestamp_freq * num_timestamps_ahead)
new_ts = pd.date_range(start=start_ts, periods=num_timestamps_ahead, freq=timestamp_freq)

# Create a dataframe with new timestamps
df_new_timestamps = pd.DataFrame({
    'ts': new_ts,
    'latitude': [np.nan] * num_timestamps_ahead,
    'longitude': [np.nan] * num_timestamps_ahead,
    'msl': [np.nan] * num_timestamps_ahead
})

# Concatenate the new timestamps dataframe with the original dataframe
df_extended = pd.concat([df_new_timestamps, df], ignore_index=True)


df_filled = df_extended.fillna(method='bfill')
df_filtered = prepare_and_apply_sanity_filters(df_filled, 0.0141, 0.0141)
print('df_filtered = ', len(df_filtered))
df_filtered = outlier_filter_for_latlon_with_msl(df_filtered)
print('df_filtered = ', len(df_filtered))


df_result = df_filtered[df_filtered['ts'] >= df['ts'].min()]
df_result = resample_df(df_result).fillna(method='ffill')
print('df_result = ', len(df_result))





