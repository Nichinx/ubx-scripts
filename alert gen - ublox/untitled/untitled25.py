# -*- coding: utf-8 -*-
"""
Created on Sun Aug 11 13:25:42 2024

@author: nichm
"""

import math
import pandas as pd
import numpy as np
import time
from datetime import datetime, time, timedelta
from pyproj import Proj, transform
import mysql.connector


db_config = {'host': 'localhost',
            'user': 'root',
            'password': 'admin123',
            'database': 'new_schema_3'
}


# Define the projection for WGS84 and UTM Zone 51N
wgs84 = Proj(proj='latlong', datum='WGS84')
utm_zone_51 = Proj(proj='utm', zone=51, datum='WGS84')
def convert_to_utm(lat, lon):
    easting, northing = transform(wgs84, utm_zone_51, lon, lat)
    return easting, northing #in meters

def euclidean_distance(x1, y1, x2, y2):
    distance = math.sqrt((x2 - x1)**2 + (y2 - y1)**2)
    distance_cm = distance * 100  # Convert to centimeters
    return distance_cm

def resample_df(df):
    df['ts'] = pd.to_datetime(df['ts'], unit='s')
    df = df.set_index('ts').resample('10min').first().reset_index()
    return df

def prepare_and_apply_sanity_filters(df, hacc=0.0141, vacc=0.0141):
    df['msl'] = np.round(df['msl'], 3)
    df = df[(df['fix_type'] == 2) & (df['sat_num'] > 28)]

    if df.empty:
        return df

    df = df[(df['hacc'] == hacc) & (df['vacc'] <= vacc)]
    df = df.reset_index(drop=True).sort_values(by='ts', ascending=True, ignore_index=True)
    return df

def outlier_filter_for_latlon_with_msl(df):
    df = df.copy()
    dfmean = df[['latitude', 'longitude', 'msl','distance_cm']].rolling(window=16, min_periods=1).mean()
    dfsd = df[['latitude', 'longitude', 'msl','distance_cm']].rolling(window=16, min_periods=1).std()

    dfulimits = dfmean + (2 * dfsd)  # 1 std
    dfllimits = dfmean - (2 * dfsd)

    df['latitude'] = df['latitude'].where((df['latitude'] <= dfulimits['latitude']) & (df['latitude'] >= dfllimits['latitude']), np.nan)
    df['longitude'] = df['longitude'].where((df['longitude'] <= dfulimits['longitude']) & (df['longitude'] >= dfllimits['longitude']), np.nan)
    df['msl'] = df['msl'].where((df['msl'] <= dfulimits['msl']) & (df['msl'] >= dfllimits['msl']), np.nan)
    df['distance_cm'] = df['distance_cm'].where((df['distance_cm'] <= dfulimits['distance_cm']) & (df['distance_cm'] >= dfllimits['distance_cm']), np.nan)

    df = df.dropna(subset=['latitude', 'longitude', 'msl','distance_cm'])
    return df

def fetch_gnss_table_names():
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        query = "SHOW TABLES LIKE 'gnss\_%'"
        cursor.execute(query)
        table_rows = cursor.fetchall()

        gnss_table_names = [table_name[0] for table_name in table_rows]
        return gnss_table_names

    except mysql.connector.Error as error:
        print(f"Error fetching GNSS table names: {error}")
        return []

    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()

def fetch_base_name_for_rover(rover_name):
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        query = "SELECT base_name FROM rover_reference_point WHERE LEFT(base_name, 3) = %s"
        cursor.execute(query, (rover_name[:3],))
        row = cursor.fetchone()

        if not row:
            return None

        return row[0]

    except mysql.connector.Error as error:
        print(f"Error fetching base name for {rover_name}: {error}")
        return None

    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()

def fetch_reference_coordinates(base_name):
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        query = "SELECT latitude, longitude FROM rover_reference_point WHERE base_name = %s"
        cursor.execute(query, (base_name,))
        row = cursor.fetchone()

        if not row:
            return None

        return row[0], row[1]

    except mysql.connector.Error as error:
        print(f"Error fetching base coordinates for {base_name}: {error}")
        return None

    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()




# # Function to process the entire existing data in the table
# def process_existing_data(table_name):
#     try:
#         connection = mysql.connector.connect(**db_config)
#         cursor = connection.cursor()

#         query = f"SELECT ts, fix_type, latitude, longitude, hacc, vacc, msl, sat_num FROM {table_name}"
#         cursor.execute(query)
#         rows = cursor.fetchall()

#         columns = ['ts', 'fix_type', 'latitude', 'longitude', 'hacc', 'vacc', 'msl', 'sat_num']
#         df = pd.DataFrame(rows, columns=columns)

#         if df.empty:
#             print(f"No data available in {table_name}.")
#             return pd.DataFrame()  # Return an empty DataFrame if no data is available

#         # Ensure no NaN values by padding with timestamps if necessary
#         df['ts'] = pd.to_datetime(df['ts'])
#         df = resample_df(df).fillna(method='ffill')

#         # Apply initial padding if necessary
#         df = apply_initial_padding(df)
        
        
        

#         # Apply sanity filters
#         df_filtered = prepare_and_apply_sanity_filters(df)
#         print('df_filtered = ', len(df_filtered))

#         # Apply outlier filters
#         df_filtered = outlier_filter_for_latlon_with_msl(df_filtered)
#         print('df_filtered = ', len(df_filtered))

#         # Final resampling and filling
#         df_result = resample_df(df_filtered).fillna(method='ffill')
#         print('df_result = ', len(df_result))

#         # Fetch the reference point and compute distances
#         rover_name = table_name.split('_')[1]  # Assuming table name format 'gnss_xxxx'
#         base_name = fetch_base_name_for_rover(rover_name)
#         if not base_name:
#             print(f"Base name not found for rover {rover_name}.")
#             return df_result

#         base_lat, base_lon = fetch_reference_coordinates(base_name)
#         if not base_lat or not base_lon:
#             print(f"Reference coordinates not found for base {base_name}.")
#             return df_result

#         base_easting, base_northing = convert_to_utm(base_lat, base_lon)

#         df_result['easting'], df_result['northing'] = zip(*df_result.apply(
#             lambda row: convert_to_utm(row['latitude'], row['longitude']), axis=1))

#         df_result['distance_cm'] = df_result.apply(
#             lambda row: euclidean_distance(row['easting'], row['northing'], base_easting, base_northing), axis=1)

#         # Further processing can be done here, such as alert generation
#         return df_result

#     except mysql.connector.Error as error:
#         print(f"Error processing GPS data from {table_name}: {error}")
#         return pd.DataFrame()

#     finally:
#         if 'connection' in locals() and connection.is_connected():
#             cursor.close()
#             connection.close()

# def apply_initial_padding(df):
#     # num_timestamps_ahead = 6
#     timestamp_freq = '10T'  # Frequency of timestamps in original data
#     rolling_window_size = 6  # Adjust this based on your actual window size

#     start_ts = df['ts'].min()

#     existing_data_before = df[df['ts'] < start_ts].shape[0]
#     num_needed_initial_ts = rolling_window_size - existing_data_before
#     num_needed_initial_ts = max(num_needed_initial_ts, 0)

#     if num_needed_initial_ts > 0:
#         start_ts = df['ts'].min() - pd.Timedelta(timestamp_freq) * num_needed_initial_ts
#         new_ts = pd.date_range(start=start_ts, periods=num_needed_initial_ts, freq=timestamp_freq)
        
#         df_new_timestamps = pd.DataFrame({
#             'ts': new_ts,
#             'latitude': [np.nan] * len(new_ts),
#             'longitude': [np.nan] * len(new_ts),
#             'msl': [np.nan] * len(new_ts)
#         })

#         df_extended = pd.concat([df_new_timestamps, df], ignore_index=True)
#         df_extended = df_extended.fillna(method='bfill')
#         return df_extended

#     return df.copy()


def fetch_all_gps_data_with_padding(table_name, rolling_window_size=6, timestamp_freq='10T'):
    all_data_instances = []  # List to store DataFrames for each instance

    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        query = f"SELECT ts, fix_type, latitude, longitude, hacc, vacc, msl, sat_num FROM {table_name}"
        cursor.execute(query)
        rows = cursor.fetchall()

        columns = ['ts', 'fix_type', 'latitude', 'longitude', 'hacc', 'vacc', 'msl', 'sat_num']
        all_data = pd.DataFrame(rows, columns=columns)
        all_data['ts'] = pd.to_datetime(all_data['ts'], unit='s')

        for _, row in all_data.iterrows():
            ts = row['ts']
            start_window = ts - timedelta(hours=4)
            end_window = ts

            # Fetch the data within the 4-hour window
            query = f"""
            SELECT ts, fix_type, latitude, longitude, hacc, vacc, msl, sat_num 
            FROM {table_name} 
            WHERE ts >= %s AND ts <= %s
            """
            cursor.execute(query, (start_window, end_window))
            window_rows = cursor.fetchall()

            # Create DataFrame for the current 4-hour window instance
            window_data = pd.DataFrame(window_rows, columns=columns)
            window_data['ts'] = pd.to_datetime(window_data['ts'], unit='s')

            # Resample the DataFrame to ensure consistent time intervals
            window_data = resample_df(window_data).fillna(method='ffill')

            # Determine how many additional timestamps are needed to reach the rolling window size
            num_needed_initial_ts = max(rolling_window_size - len(window_data), 0)

            if num_needed_initial_ts > 0:
                # Generate extra timestamps ahead of the initial timestamp if not enough data
                extra_ts = pd.date_range(start=start_window - pd.Timedelta(timestamp_freq) * num_needed_initial_ts,
                                         periods=num_needed_initial_ts, freq=timestamp_freq)
                extra_data = pd.DataFrame({
                    'ts': extra_ts,
                    'latitude': np.nan,
                    'longitude': np.nan,
                    'msl': np.nan
                })
                
                # Concatenate extra timestamps with the existing window data
                window_data = pd.concat([extra_data, window_data], ignore_index=True)
                window_data = window_data.sort_values(by='ts').reset_index(drop=True)       
                window_data = window_data.fillna(method='bfill')

            # Ensure that we don't include timestamps beyond the current instance's timestamp
            window_data = window_data[window_data['ts'] <= end_window]
            window_data['original_ts'] = ts  # Add original timestamp for reference

            # Get base reference coordinates based on table name
            base_name = fetch_base_name_for_rover(table_name)
            if base_name:
                base_coords = fetch_reference_coordinates(base_name)
                if base_coords:
                    base_lat, base_lon = base_coords
                    base_lat, base_lon = convert_to_utm(base_lat, base_lon)

                    # Calculate distances
                    window_data['easting'], window_data['northing'] = zip(*window_data.apply(
                        lambda row: convert_to_utm(row['latitude'], row['longitude']), axis=1))

                    window_data['distance_cm'] = window_data.apply(
                        lambda row: euclidean_distance(row['easting'], row['northing'], base_lat, base_lon), axis=1)


            # Apply filters (assumed to be defined elsewhere)
            window_data = prepare_and_apply_sanity_filters(window_data)
            window_data = outlier_filter_for_latlon_with_msl(window_data)

            # Resample and forward fill again after applying filters
            window_data = resample_df(window_data).fillna(method='ffill')

            # Add the processed DataFrame to the list of instances
            all_data_instances.append(window_data)

    except mysql.connector.Error as error:
        print(f"Error fetching GPS data from {table_name}: {error}")

    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()

    return all_data_instances



