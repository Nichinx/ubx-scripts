# -*- coding: utf-8 -*-
"""
Created on Tue Jul 23 11:28:13 2024

@author: nichm
"""

import mysql.connector
import math
import numpy as np
import pandas as pd
import time
from datetime import datetime, time, timedelta
from pyproj import Proj, transform


db_config = {'host': 'localhost',
            'user': 'root',
            'password': 'admin123',
            'database': 'new_schema_2'
}

horizontal_accuracy = 0.0141
vertical_accuracy = 0.01205


def prepare_and_apply_sanity_filters(df, horizontal_accuracy, vertical_accuracy):
    df['msl'] = np.round(df['msl'], 2)
    df = df[(df['fix_type'] == 2) & (df['sat_num'] > 28)]

    if df.empty:
        return df

    df = df[(df['hacc'] == horizontal_accuracy) & (df['vacc'] <= vertical_accuracy)]
    df = df.reset_index(drop=True).sort_values(by='ts', ascending=True, ignore_index=True)
    return df

def outlier_filter_for_latlon_with_msl(df):
    df = df.copy()
    dfmean = df[['latitude', 'longitude', 'msl']].rolling(window=24, min_periods=1).mean()
    dfsd = df[['latitude', 'longitude', 'msl']].rolling(window=24, min_periods=1).std()

    dfulimits = dfmean + (1 * dfsd)
    dfllimits = dfmean - (1 * dfsd)

    df['latitude'] = df['latitude'].where((df['latitude'] <= dfulimits['latitude']) & (df['latitude'] >= dfllimits['latitude']), np.nan)
    df['longitude'] = df['longitude'].where((df['longitude'] <= dfulimits['longitude']) & (df['longitude'] >= dfllimits['longitude']), np.nan)
    df['msl'] = df['msl'].where((df['msl'] <= dfulimits['msl']) & (df['msl'] >= dfllimits['msl']), np.nan)

    df = df.dropna(subset=['latitude', 'longitude', 'msl'])
    return df

def resample_df(df):
    df['ts'] = pd.to_datetime(df['ts'], unit='s')
    df = df.set_index('ts').resample('30min').mean().reset_index()
    return df


wgs84 = Proj(proj='latlong', datum='WGS84')
utm_zone_51 = Proj(proj='utm', zone=51, datum='WGS84')

def convert_to_utm(lat, lon):
    easting, northing = transform(wgs84, utm_zone_51, lon, lat)
    return easting, northing

def euclidean_distance(x1, y1, x2, y2):
    distance = math.sqrt((x2 - x1)**2 + (y2 - y1)**2)
    distance_cm = distance * 100
    return distance_cm

# def euclidean_distance(lat1, lon1, lat2, lon2):
#     lat1_rad = math.radians(lat1)
#     lon1_rad = math.radians(lon1)
#     lat2_rad = math.radians(lat2)
#     lon2_rad = math.radians(lon2)

#     R = 6371000
#     delta_lat = lat2_rad - lat1_rad
#     delta_lon = lon2_rad - lon1_rad

#     distance = math.sqrt((delta_lat * R)**2 + \
#                     (delta_lon * R * math.cos((lat1_rad + lat2_rad) / 2))**2)
#     distance_cm = distance * 100
#     return distance_cm


def fetch_data_with_offset(table_name):
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        # Fetch the most recent timestamp from the table
        query_latest_ts = f"SELECT ts FROM {table_name} ORDER BY ts DESC LIMIT 1"
        cursor.execute(query_latest_ts)
        latest_ts_row = cursor.fetchone()

        if not latest_ts_row:
            print(f"No data available in {table_name}.")
            return pd.DataFrame()  # Return an empty DataFrame if no data is available

        latest_ts = latest_ts_row[0]

        # Calculate the start_time considering the 8-hour window for filtering
        offset_time = latest_ts - timedelta(hours=8)  # 8-hour window for the filter
        target_start_time = latest_ts - timedelta(hours=4)  # 4-hour window for the intended data

        # Fetch data within the extended 8-hour window
        query = f"""
        SELECT ts, fix_type, latitude, longitude, hacc, vacc, msl, sat_num 
        FROM {table_name} 
        WHERE ts >= %s AND ts <= %s
        """
        cursor.execute(query, (offset_time, latest_ts))
        rows = cursor.fetchall()

        columns = ['ts', 'fix_type', 'latitude', 'longitude', 'hacc', 'vacc', 'msl', 'sat_num']
        extended_data = pd.DataFrame(rows, columns=columns)

        if extended_data.empty:
            print(f"No data found in the 8-hour window from {latest_ts}.")
            return pd.DataFrame()

        # Apply the outlier filter (you can include other filters as needed)
        filtered_data = outlier_filter(extended_data)

        # Filter again to include only the last 4 hours of data after filtering
        final_data = filtered_data[filtered_data['ts'] >= target_start_time]

        return final_data

    except mysql.connector.Error as error:
        print(f"Error fetching GPS data from {table_name}: {error}")
        return pd.DataFrame()

    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()


