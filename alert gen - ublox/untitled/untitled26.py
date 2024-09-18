# -*- coding: utf-8 -*-
"""
Created on Fri Sep 13 14:29:21 2024

@author: nichm
"""

import math
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pyproj import Proj, transform
import mysql.connector
import statsmodels.api as sm
from statsmodels.regression.rolling import RollingOLS

db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'admin123',
    'database': 'new_schema_3'
}

# Define the projection for WGS84 and UTM Zone 51N
wgs84 = Proj(proj='latlong', datum='WGS84')
utm_zone_51 = Proj(proj='utm', zone=51, datum='WGS84')


def create_db_connection():
    """Create and return a MySQL database connection."""
    try:
        connection = mysql.connector.connect(**db_config)
        return connection
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None

def close_db_connection(connection):
    """Close the database connection."""
    if connection.is_connected():
        connection.close()

def resample_df(df):
    df['ts'] = pd.to_datetime(df['ts'], unit='s')
    df = df.set_index('ts').resample('30min').mean().reset_index()
    return df

def sanity_filters(df, hacc=0.0141, vacc=0.0141):
    df['msl'] = np.round(df['msl'], 3)
    df = df[(df['fix_type'] == 2) & (df['sat_num'] > 28)]

    if df.empty:
        return df

    df = df[(df['hacc'] == hacc) & (df['vacc'] <= vacc)]
    df = df.reset_index(drop=True).sort_values(by='ts', ascending=True, ignore_index=True)
    return df

# def outlier_filter_for_latlon_with_msl(df):
#     df = df.copy()
#     dfmean = df[['latitude', 'longitude', 'msl', 'distance_cm']].rolling(window=16, min_periods=1).mean()
#     dfsd = df[['latitude', 'longitude', 'msl', 'distance_cm']].rolling(window=16, min_periods=1).std()

#     dfulimits = dfmean + (2 * dfsd)
#     dfllimits = dfmean - (2 * dfsd)

#     df['latitude'] = df['latitude'].where((df['latitude'] <= dfulimits['latitude']) & (df['latitude'] >= dfllimits['latitude']), np.nan)
#     df['longitude'] = df['longitude'].where((df['longitude'] <= dfulimits['longitude']) & (df['longitude'] >= dfllimits['longitude']), np.nan)
#     df['msl'] = df['msl'].where((df['msl'] <= dfulimits['msl']) & (df['msl'] >= dfllimits['msl']), np.nan)
#     df['distance_cm'] = df['distance_cm'].where((df['distance_cm'] <= dfulimits['distance_cm']) & (df['distance_cm'] >= dfllimits['distance_cm']), np.nan)

#     df = df.dropna(subset=['latitude', 'longitude', 'msl', 'distance_cm'])
#     return df

def outlier_filter_for_latlon_with_msl(df):
    df = df.copy()
    dfmean = df[['easting', 'northing', 'msl', 'distance_cm']].rolling(window=16, min_periods=1).mean()
    dfsd = df[['easting', 'northing', 'msl', 'distance_cm']].rolling(window=16, min_periods=1).std()

    dfulimits = dfmean + (2 * dfsd)
    dfllimits = dfmean - (2 * dfsd)

    df['easting'] = df['easting'].where((df['easting'] <= dfulimits['easting']) & (df['easting'] >= dfllimits['easting']), np.nan)
    df['northing'] = df['northing'].where((df['northing'] <= dfulimits['northing']) & (df['northing'] >= dfllimits['northing']), np.nan)
    df['msl'] = df['msl'].where((df['msl'] <= dfulimits['msl']) & (df['msl'] >= dfllimits['msl']), np.nan)
    df['distance_cm'] = df['distance_cm'].where((df['distance_cm'] <= dfulimits['distance_cm']) & (df['distance_cm'] >= dfllimits['distance_cm']), np.nan)

    df = df.dropna(subset=['northing', 'easting', 'msl', 'distance_cm'])
    return df

def fetch_gnss_rover_table_names():
    """Fetch GNSS rover table names from the database."""
    connection = create_db_connection()
    if connection:
        with connection.cursor() as cursor:
            cursor.execute("SHOW TABLES LIKE 'gnss_%';")
            tables = cursor.fetchall()
        close_db_connection(connection)
        return [table[0] for table in tables]
    return []

def get_rover_name(table_name):
    return table_name.replace('gnss_', '')

def get_rover_reference_point(rover_name):
    """Get the rover reference point (latitude, longitude)."""
    connection = create_db_connection()
    if connection:
        with connection.cursor() as cursor:
            query = f"SELECT latitude, longitude FROM rover_reference_point WHERE rover_name = '{rover_name}';"
            cursor.execute(query)
            result = cursor.fetchone()
        close_db_connection(connection)
        return result if result else (None, None)
    return (None, None)

def convert_to_utm(lat, lon):
    easting, northing = transform(wgs84, utm_zone_51, lon, lat)
    return easting, northing  # in meters

def euclidean_distance(lat, lon, ref_lat, ref_lon):
    ref_easting, ref_northing = convert_to_utm(ref_lat, ref_lon)
    distance = math.sqrt((ref_easting - lat) ** 2 + (ref_northing - lon) ** 2)
    distance_cm = distance * 100  # Convert to centimeters
    return distance_cm

# def get_gnss_data(table_name, start_time):
#     """Fetch GNSS data from the table."""
#     connection = create_db_connection()
#     if connection:
#         end_time = start_time + timedelta(hours=8)
#         query = f"SELECT * FROM {table_name} WHERE ts BETWEEN '{start_time}' AND '{end_time}';"
#         df = pd.read_sql(query, connection)
#         close_db_connection(connection)
#         return df
#     return pd.DataFrame()

def get_gnss_data(table_name, start_time, end_time):
    """Fetch GNSS data from the table for the given time window."""
    connection = create_db_connection()
    if connection:
        query = f"SELECT * FROM {table_name} WHERE ts BETWEEN '{start_time}' AND '{end_time}';"
        df = pd.read_sql(query, connection)
        close_db_connection(connection)
        return df
    return pd.DataFrame()

def fetch_all_ts_data(table_name):
    """Fetch all distinct timestamps and process data with a start time of ts - 8 hours."""
    connection = create_db_connection()
    
    # Get all distinct timestamps from the table
    if connection:
        query = f"SELECT DISTINCT ts FROM {table_name} ORDER BY ts;"
        all_ts = pd.read_sql(query, connection)['ts']
        close_db_connection(connection)
    
    dataframes = []
    
    # Iterate over all timestamps
    for ts in all_ts:
        start_time = ts - timedelta(hours=8)
        end_time = ts
        df = get_gnss_data(table_name, start_time, end_time)
        
        # Append the resulting dataframe to the list if it's not empty
        if not df.empty:
            dataframes.append(df)
    
    return dataframes  # List of dataframes, each for an 8-hour window

# def fetch_all_gnss_data(table_name):
#     all_data_instances = []  # List to store DataFrames for each instance

#     try:
#         # Connect to the database
#         connection = mysql.connector.connect(**db_config)
#         cursor = connection.cursor()

#         # Query to fetch all timestamps and data
#         query = f"SELECT ts, fix_type, latitude, longitude, hacc, vacc, msl, sat_num FROM {table_name}"
#         cursor.execute(query)
#         rows = cursor.fetchall()

#         # Define column names
#         columns = ['ts', 'fix_type', 'latitude', 'longitude', 'hacc', 'vacc', 'msl', 'sat_num']
#         all_data = pd.DataFrame(rows, columns=columns)

#         # Iterate through each row and fetch the previous 8 hours of data
#         for _, row in all_data.iterrows():
#             ts = row['ts']
#             start_time = ts - timedelta(hours=8)
            
#             # Convert timestamps to string representation in MySQL format
#             ts_str = ts.strftime('%Y-%m-%d %H:%M:%S')
#             start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
            
#             # Adjust the query to fetch data within the 8-hour window
#             query = f"""
#             SELECT ts, fix_type, latitude, longitude, hacc, vacc, msl, sat_num 
#             FROM {table_name} 
#             WHERE ts >= %s AND ts <= %s
#             """
#             cursor.execute(query, (start_time_str, ts_str))
#             previous_rows = cursor.fetchall()

#             # Create a new DataFrame for each instance with the original and previous data
#             instance_data = pd.DataFrame(previous_rows, columns=columns)
#             instance_data['original_ts'] = ts  # Add original timestamp for reference
#             all_data_instances.append(instance_data)

#     except mysql.connector.Error as error:
#         print(f"Error fetching GNSS data from {table_name}: {error}")

#     finally:
#         if 'connection' in locals() and connection.is_connected():
#             cursor.close()
#             connection.close()

#     return all_data_instances


def apply_filters(df):
    df_filtered = sanity_filters(df)
    if not df_filtered.empty:
        df_filtered = outlier_filter_for_latlon_with_msl(df_filtered)
    return df_filtered

# def compute_velocity(df):
#     df['ts'] = pd.to_datetime(df['ts'], unit='s')
#     df.set_index('ts', inplace=True)

#     # df['mx'] = df['easting'].rolling(window='4H').apply(lambda x: np.polyfit(range(len(x)), x, 1)[0], raw=True)
#     # df['my'] = df['northing'].rolling(window='4H').apply(lambda x: np.polyfit(range(len(x)), x, 1)[0], raw=True)
    
#     easting_model = RollingOLS(y=df['easting'], x=4, window=8)
#     northing_model = RollingOLS(y=df['northing'], x=4, window=8)
    
#     df['mx'] = easting_model.beta
#     df['my'] = northing_model.beta
#     df['intercept_easting'] = easting_model.alpha
#     df['intercept_northing'] = northing_model.alpha

#     df['velocity_cm_hr'] = np.sqrt(df['mx'] ** 2 + df['my'] ** 2) * 100  # Convert to centimeters
#     df['theta'] = np.arcsin(df['mx'] / df['velocity_cm_hr'])

#     return df

def compute_rolling_velocity(df, time_col='timestamp', northing_col='northing', easting_col='easting', window=8):
    """
    Computes the rolling slopes of Northing (latitude UTM) and Easting (longitude UTM) using Rolling OLS,
    and then calculates the velocity by combining these slope components.

    Parameters:
    df (pd.DataFrame): DataFrame containing the UTM coordinates and timestamps.
    time_col (str): The column name for the timestamp (default is 'timestamp').
    northing_col (str): The column name for Northing (latitude in UTM).
    easting_col (str): The column name for Easting (longitude in UTM).
    window (int): The window size for the rolling OLS regression (default is 8 data points).

    Returns:
    pd.DataFrame: DataFrame with the calculated slopes for Northing, Easting, and the resulting velocity.
    """
    
    # Step 1: Ensure the dataframe is sorted by timestamp
    df = df.sort_values(by=time_col).copy()

    # Step 2: Convert timestamp to a numeric value for regression
    df['timestamp_numeric'] = pd.to_numeric(df[time_col])

    # Step 3: Prepare independent variable (timestamp) with constant term for OLS
    X = sm.add_constant(df['timestamp_numeric'])

    # Step 4: Apply Rolling OLS for Northing
    rolling_model_northing = RollingOLS(df[northing_col], X, window=window).fit()
    df['northing_slope'] = rolling_model_northing.params['timestamp_numeric']

    # Step 5: Apply Rolling OLS for Easting
    rolling_model_easting = RollingOLS(df[easting_col], X, window=window).fit()
    df['easting_slope'] = rolling_model_easting.params['timestamp_numeric']

    # Step 6: Calculate velocity using the slopes (Pythagorean theorem)
    df['velocity'] = np.sqrt(df['northing_slope']**2 + df['easting_slope']**2)

    # Convert velocity to centimeters per hour (assuming slopes are meters per second)
    df['velocity_cm_per_hour'] = df['velocity'] * 360000

    return df[['timestamp', 'northing_slope', 'easting_slope', 'velocity_cm_per_hour']]

def update_stored_data(table_name, df):
    """Update stored GNSS data in the database."""
    connection = create_db_connection()
    if connection:
        df['ts_written'] = pd.Timestamp.now()
        with connection.cursor() as cursor:
            for index, row in df.iterrows():
                query = f"""
                INSERT INTO stored_dist_gnss_{table_name.replace('gnss_', '')} 
                (ts, ts_written, dist_from_ref, velocity_cm_hr, alert_level)
                VALUES ('{row['ts']}', '{row['ts_written']}', {row['dist_from_ref']}, {row['velocity_cm_hr']}, {row['alert_level']})
                """
                cursor.execute(query)
            connection.commit()
        close_db_connection(connection)

def check_alerts(df):
    df['alert_level'] = 0
    df.loc[df['velocity_cm_hr'] > 0.25, 'alert_level'] = 2  # Alert level 2
    df.loc[df['velocity_cm_hr'] > 1.8, 'alert_level'] = 3  # Alert level 3
    df['alert_level'].fillna(-1, inplace=True)  # -1 if no data
    return df

def process_gnss_data():
    """Main process to fetch GNSS data, apply filters, compute velocity, and check alerts."""
    gnss_tables = fetch_gnss_rover_table_names()

    for table_name in gnss_tables:
        rover_name = get_rover_name(table_name)
        ref_lat, ref_lon = get_rover_reference_point(rover_name)
        ref_easting, ref_northing = convert_to_utm(ref_lat, ref_lon)

        # latest_data = pd.read_sql(f"SELECT MAX(ts) FROM {table_name};", create_db_connection()).iloc[0, 0]
        # start_time = latest_data - timedelta(hours=8)

        # df = get_gnss_data(table_name, start_time)
        
        df = fetch_all_ts_data(table_name)
        df[['easting', 'northing']] = df.apply(lambda row: convert_to_utm(row['latitude'], row['longitude']), axis=1, result_type='expand')
        # df['easting_relative'] = df['easting'] - ref_easting
        # df['northing_relative'] = df['northing'] - ref_northing
        df['distance_cm'] = df.apply(lambda row: euclidean_distance(row['easting'], row['northing'], ref_lat, ref_lon), axis=1)

        # df = df.drop(columns=['latitude','longitude','temp','volt'])
        df_filtered = apply_filters(df)
        df_filtered = resample_df(df_filtered).fillna(method='ffill')

        # df_filtered = compute_velocity(df_filtered)
        # df_filtered = check_alerts(df_filtered)

        # update_stored_data(table_name, df_filtered)
