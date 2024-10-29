# -*- coding: utf-8 -*-
"""
Created on Fri Sep 13 14:29:21 2024

@author: nichm
"""

import math
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
# from pyproj import Proj, transform
from pyproj import Transformer
# import mysql.connector
from sqlalchemy import create_engine
from sqlalchemy.engine import Connection
import statsmodels.api as sm
from statsmodels.regression.rolling import RollingOLS
import matplotlib.pyplot as plt

db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'admin123',
    'database': 'new_schema_3'
}

# Define the projection for WGS84 and UTM Zone 51N
# wgs84 = Proj(proj='latlong', datum='WGS84')
# utm_zone_51 = Proj(proj='utm', zone=51, datum='WGS84')
transformer = Transformer.from_crs("epsg:4326", "epsg:32651", always_xy=True)
# transformer = Transformer.from_crs("epsg:4326", "epsg:32651")

# def create_db_connection():
#     """Create and return a MySQL database connection."""
#     try:
#         connection = mysql.connector.connect(**db_config)
#         return connection
#     except mysql.connector.Error as err:
#         print(f"Error: {err}")
#         return None

# def close_db_connection(connection):
#     """Close the database connection."""
#     if connection.is_connected():
#         connection.close()

def create_db_connection():
    """Create and return a SQLAlchemy database connection (engine)."""
    try:
        connection_string = f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config.get('port', 3306)}/{db_config['database']}"
        engine = create_engine(connection_string)
        return engine.connect() 
    except Exception as err:
        print(f"Error: {err}")
        return None

def close_db_connection(connection: Connection):
    """Close the database connection."""
    if connection:
        connection.close()

def resample_df(df):
    df['ts'] = pd.to_datetime(df['ts'])
    df = df.set_index('ts').resample('30min').mean().reset_index()
    return df

def sanity_filters(df, hacc=0.0141, vacc=0.0141):
    df['msl'] = np.round(df['msl'], 3)
    df = df[(df['fix_type'] == 2) & (df['sat_num'] > 20)]

    if df.empty:
        return df

    df = df[(df['hacc'] == hacc) & (df['vacc'] <= vacc)]
    df = df.reset_index(drop=True).sort_values(by='ts', ascending=True, ignore_index=True)
    return df

def outlier_filter_for_latlon_with_msl(df):
    df = df.copy()
    dfmean = df[['easting', 'northing', 'msl', 'distance_cm']].rolling(window=12, min_periods=1).mean()
    dfsd = df[['easting', 'northing', 'msl', 'distance_cm']].rolling(window=12, min_periods=1).std()

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
        query = "SHOW TABLES LIKE %s"
        tables = pd.read_sql(query, connection, params=('gnss_%',))
        close_db_connection(connection)
        return tables.iloc[:, 0].tolist()
    return []

def get_rover_name(table_name):
    return table_name.replace('gnss_', '')

def get_rover_reference_point(rover_name):
    """Get the rover reference point (latitude, longitude)."""
    connection = create_db_connection()
    if connection:
        query = f"SELECT latitude, longitude FROM rover_reference_point WHERE rover_name = '{rover_name}';"
        result = pd.read_sql(query, connection)
        close_db_connection(connection)
        return result.iloc[0].tolist() if not result.empty else (None, None)
    return (None, None)

def convert_to_utm(lon, lat):
    easting, northing = transformer.transform(lon, lat)
    return easting, northing  # in meters

# def euclidean_distance(lon, lat, ref_lon, ref_lat):
#     # ref_easting, ref_northing = convert_to_utm(ref_lon, ref_lat)
#     # distance = math.sqrt((ref_easting - lat) ** 2 + (ref_northing - lon) ** 2)
#     distance = math.sqrt((ref_lon - lon) ** 2 + (ref_lat - lat) ** 2)
#     distance_cm = distance * 100  # Convert to centimeters
#     return distance_cm

def euclidean_distance(easting, northing, ref_easting, ref_northing):
    distance = math.sqrt((easting - ref_easting) ** 2 + (northing - ref_northing) ** 2)
    distance_cm = distance * 100  # Convert to centimeters
    return distance_cm

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
    """Fetch all distinct timestamps and process data with a start time of ts - 12 hours."""
    connection = create_db_connection()
    
    if connection:
        query = f"SELECT DISTINCT ts FROM {table_name} ORDER BY ts;"
        all_ts = pd.read_sql(query, connection)['ts']
        all_ts = pd.to_datetime(all_ts)
        close_db_connection(connection)
    
    dataframes = []
    for ts in all_ts:
        start_time = ts - timedelta(hours=12)
        end_time = ts
        df = get_gnss_data(table_name, start_time, end_time)

        if not df.empty:
            dataframes.append(df)
    
    return dataframes 

def apply_filters(df):
    df_filtered = sanity_filters(df)
    if not df_filtered.empty:
        df_filtered = outlier_filter_for_latlon_with_msl(df_filtered)
    return df_filtered

def compute_rolling_velocity(df, time_col='ts', northing_col='northing_diff', easting_col='easting_diff', window=8, plot=True):
    df = df.sort_values(by=time_col).copy()
    df['ts'] = pd.to_datetime(df['ts'])
    
    if len(df) < window:
       # print(f"DataFrame has less than {window} rows, rolling OLS cannot be computed.")
       df['northing_slope'] = np.nan
       df['easting_slope'] = np.nan
       df['velocity_cm_hr'] = np.nan
       return df[['ts', 'northing_slope', 'easting_slope', 'velocity_cm_hr']]
   
    df['timestamp_numeric'] = pd.to_numeric(df[time_col])/1e9 #nanoseconds to seconds
    X = sm.add_constant(df['timestamp_numeric'])
    
    rolling_model_northing = RollingOLS(df[northing_col], X, window=window).fit()
    df['northing_slope'] = rolling_model_northing.params['timestamp_numeric']

    rolling_model_easting = RollingOLS(df[easting_col], X, window=window).fit()
    df['easting_slope'] = rolling_model_easting.params['timestamp_numeric']

    df['velocity'] = np.sqrt(df['northing_slope']**2 + df['easting_slope']**2)
    df['velocity_cm_hr'] = df['velocity'] * 360000
    
    
    # Optional plotting
    if plot:
        # Plot for Northing
        plt.figure(figsize=(10, 6))
        
        # Scatter plot for actual northing_diff points
        plt.scatter(df['ts'], df[northing_col], label='Northing Points', color='blue', alpha=0.5)
        
        # Line plot for the rolling slope (best fit line)
        plt.plot(df['ts'], df['northing_slope'] * (df['timestamp_numeric'] - df['timestamp_numeric'].mean()) + df[northing_col].mean(), 
                 label='Rolling Regression (Northing Slope)', color='red', linewidth=2)
    
        plt.xlabel('Timestamp')
        plt.ylabel('Northing Diff')
        plt.title('Northing Diff vs Timestamp with Rolling Slope')
        plt.grid(True)
        plt.xticks(rotation=45)
        plt.legend()
        plt.tight_layout()
        plt.show()
    
        # Plot for Easting
        plt.figure(figsize=(10, 6))
        
        # Scatter plot for actual easting_diff points
        plt.scatter(df['ts'], df[easting_col], label='Easting Points', color='green', alpha=0.5)
        
        # Line plot for the rolling slope (best fit line)
        plt.plot(df['ts'], df['easting_slope'] * (df['timestamp_numeric'] - df['timestamp_numeric'].mean()) + df[easting_col].mean(), 
                 label='Rolling Regression (Easting Slope)', color='orange', linewidth=2)
    
        plt.xlabel('Timestamp')
        plt.ylabel('Easting Diff')
        plt.title('Easting Diff vs Timestamp with Rolling Slope')
        plt.grid(True)
        plt.xticks(rotation=45)
        plt.legend()
        plt.tight_layout()
        plt.show()
    
    
    return df[['ts', 'northing_slope', 'easting_slope', 'velocity_cm_hr']]


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

# def process_gnss_data():
#     """Main process to fetch GNSS data, apply filters, compute velocity, and check alerts."""
#     gnss_tables = fetch_gnss_rover_table_names()

#     for table_name in gnss_tables:
#         rover_name = get_rover_name(table_name)
#         ref_lat, ref_lon = get_rover_reference_point(rover_name)
#         ref_easting, ref_northing = convert_to_utm(ref_lat, ref_lon)

#         # latest_data = pd.read_sql(f"SELECT MAX(ts) FROM {table_name};", create_db_connection()).iloc[0, 0]
#         # start_time = latest_data - timedelta(hours=8)

#         # df = get_gnss_data(table_name, start_time)
        
#         df = fetch_all_ts_data(table_name)
#         df[['easting', 'northing']] = df.apply(lambda row: convert_to_utm(row['latitude'], row['longitude']), axis=1, result_type='expand')
#         # df['easting_relative'] = df['easting'] - ref_easting
#         # df['northing_relative'] = df['northing'] - ref_northing
#         df['distance_cm'] = df.apply(lambda row: euclidean_distance(row['easting'], row['northing'], ref_lat, ref_lon), axis=1)

#         # df = df.drop(columns=['latitude','longitude','temp','volt'])
#         df_filtered = apply_filters(df)
#         df_filtered = resample_df(df_filtered).fillna(method='ffill')

def process_gnss_data():
    """Main process to fetch GNSS data, apply filters, compute velocity, and check alerts."""
    gnss_tables = fetch_gnss_rover_table_names()

    for table_name in gnss_tables:
        rover_name = get_rover_name(table_name)
        ref_lat, ref_lon = get_rover_reference_point(rover_name)
        ref_easting, ref_northing = convert_to_utm(ref_lon, ref_lat)

        dataframes = fetch_all_ts_data(table_name)

        for df in dataframes:
            # Convert lat/lon to UTM and compute distance from the reference point
            df[['easting', 'northing']] = df.apply(lambda row: convert_to_utm(row['longitude'], row['latitude']), axis=1, result_type='expand')
            
            # Compute difference from the reference point
            df['easting_diff'] = df['easting'] - ref_easting
            df['northing_diff'] = df['northing'] - ref_northing
            
            df['distance_cm'] = df.apply(lambda row: euclidean_distance(row['easting'], row['northing'], ref_easting, ref_northing), axis=1)

            # Apply filters
            df_filtered = apply_filters(df)
            df_filtered = resample_df(df_filtered).fillna(method='ffill')
            
            # df_filtered = df_filtered.drop(columns=['fix_type','latitude','longitude','hacc','vacc','temp','volt'])

            # Compute velocity based on the rolling window (4-hour window equivalent)
            df_velocity = compute_rolling_velocity(df_filtered)

            # # Check for alert levels
            df_alerts = check_alerts(df_velocity)
            # print(df_alerts)

            # # Update the database with the processed data
            # update_stored_data(table_name, df_alerts)
            
            