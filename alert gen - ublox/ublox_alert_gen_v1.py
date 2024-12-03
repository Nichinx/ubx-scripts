# -*- coding: utf-8 -*-
"""
Created on Fri Sep 13 14:29:21 2024

@author: nichm
"""

import math
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pyproj import Transformer
from sqlalchemy import create_engine
from sqlalchemy.engine import Connection
import statsmodels.api as sm
from statsmodels.regression.rolling import RollingOLS
import matplotlib.pyplot as plt

db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'admin123',
    'database': 'new_schema_2'
}

# Define the projection for WGS84 and UTM Zone 51N
transformer = Transformer.from_crs("epsg:4326", "epsg:32651", always_xy=True)

def create_db_connection():
    try:
        connection_string = f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config.get('port', 3306)}/{db_config['database']}"
        engine = create_engine(connection_string)
        return engine.connect() 
    except Exception as err:
        print(f"Error: {err}")
        return None

def close_db_connection(connection: Connection):
    if connection:
        connection.close()

def resample_df(df):
    df['ts'] = pd.to_datetime(df['ts'])
    return df.set_index('ts').resample('30min').mean().reset_index()

def sanity_filters(df, hacc=0.0141, vacc=0.0141):
    df['msl'] = np.round(df['msl'], 3)
    df = df[(df['fix_type'] == 2) & (df['sat_num'] > 20)]

    if df.empty:
        return df

    df = df[(df['hacc'] == hacc) & (df['vacc'] <= vacc)]
    return df.reset_index(drop=True).sort_values(by='ts', ascending=True, ignore_index=True)


def outlier_filter_for_latlon(df, freq='30min', rolling_window='12H', threshold=1.4):
    """
    Apply outlier filtering on latitude and longitude data using rolling statistics.

    Parameters:
    - df: DataFrame containing 'easting', 'northing', and 'distance_cm' columns.
    - freq: Resampling frequency (default is '30min').
    - rolling_window: Rolling window size as a time period (default is '12H').
    - threshold: Threshold multiplier for the rolling standard deviation (default is 1.4).

    Returns:
    - Filtered DataFrame with outliers removed.
    """

    df = df.set_index('ts').resample(freq).mean().reset_index()
    
    rolling_window_rows = int(pd.Timedelta(rolling_window) / pd.Timedelta(freq))    #24 window size
    if len(df) < rolling_window_rows:
        print("DataFrame length is less than the rolling window size. Returning unfiltered DataFrame.")
        return df
    
    df = df.dropna(subset=['easting', 'northing', 'distance_cm'])
    rolling_mean = df[['easting', 'northing', 'distance_cm']].rolling(window=rolling_window_rows, min_periods=1).mean()
    rolling_std = df[['easting', 'northing', 'distance_cm']].rolling(window=rolling_window_rows, min_periods=1).std()
    for col in ['easting', 'northing', 'distance_cm']:
        upper_limit = rolling_mean[col] + threshold * rolling_std[col]
        lower_limit = rolling_mean[col] - threshold * rolling_std[col]
        df[col] = df[col].where((df[col] <= upper_limit) & (df[col] >= lower_limit), np.nan)
    return df.dropna(subset=['easting', 'northing', 'distance_cm'])

def fetch_gnss_rover_table_names():
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
    connection = create_db_connection()
    if connection:
        try:
            query = f"""
                SELECT rover_id, latitude, longitude 
                FROM rover_reference_point 
                WHERE rover_name = '{rover_name}';
            """
            result = pd.read_sql(query, connection)
            close_db_connection(connection)
            if not result.empty:
                return result.iloc[0]['rover_id'], result.iloc[0]['latitude'], result.iloc[0]['longitude']
        except Exception as e:
            print(f"Error fetching rover reference point: {e}")
        finally:
            close_db_connection(connection)
    return None, None, None

def convert_to_utm(lon, lat):
    return transformer.transform(lon, lat) # in meters

def euclidean_distance(easting, northing, ref_easting, ref_northing):
    return math.sqrt((easting - ref_easting) ** 2 + (northing - ref_northing) ** 2) * 100 # Convert to centimeters

def get_gnss_data(table_name, start_time, end_time):
    connection = create_db_connection()
    if connection:
        query = f"SELECT * FROM {table_name} WHERE ts BETWEEN '{start_time}' AND '{end_time}';"
        df = pd.read_sql(query, connection)
        close_db_connection(connection)
        return df
    return pd.DataFrame()

def fetch_all_ts_data(table_name):
    connection = create_db_connection()
    if connection:
        query = f"SELECT * FROM {table_name} ORDER BY ts;"
        dataframes = pd.read_sql(query, connection)
        close_db_connection(connection)
        return dataframes
    return pd.DataFrame()

def apply_filters(df):
    df_filtered = sanity_filters(df)
    return outlier_filter_for_latlon(df_filtered) if not df_filtered.empty else df_filtered

def compute_rolling_velocity(df, time_col='ts', northing_col='northing_diff', easting_col='easting_diff', window=16, plot=True):
    df = df.sort_values(by=time_col).copy()
    df['ts'] = pd.to_datetime(df['ts'])
    
    if len(df) < window:
        df['northing_slope'] = df['easting_slope'] = df['velocity_cm_hr'] = np.nan
        return df[['ts', 'northing_slope', 'easting_slope', 'velocity_cm_hr']]
    
    df['timestamp_numeric'] = pd.to_numeric(df[time_col]) / 1e9  # nanoseconds to seconds
    X = sm.add_constant(df['timestamp_numeric'])
    
    # Rolling OLS 
    rolling_model_northing = RollingOLS(df[northing_col], X, window=window).fit()
    rolling_model_easting = RollingOLS(df[easting_col], X, window=window).fit()

    df['northing_slope'] = rolling_model_northing.params['timestamp_numeric']
    df['easting_slope'] = rolling_model_easting.params['timestamp_numeric']
    df['velocity'] = np.sqrt(df['northing_slope'] ** 2 + df['easting_slope'] ** 2)
    df['velocity_cm_hr'] = df['velocity'] * 360000  # Convert m/s to cm/hr
    
    df['best_fit_northing'] = np.nan
    df['best_fit_easting'] = np.nan
    for i in range(window - 1, len(df)):
        # Get the current window
        window_df = df.iloc[i - window + 1:i + 1]
        if len(window_df) == window:
            X_window = sm.add_constant(window_df['timestamp_numeric'])
            model_northing = sm.OLS(window_df[northing_col], X_window).fit()
            model_easting = sm.OLS(window_df[easting_col], X_window).fit()
            
            # Calculate the best fit line using the model parameters
            df.loc[i, 'best_fit_northing'] = model_northing.predict(X_window).mean()  # mean for the last timestamp in window
            df.loc[i, 'best_fit_easting'] = model_easting.predict(X_window).mean()  # mean for the last timestamp in window

    fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(12, 9), sharex=True)
    fig.suptitle(f"plot {i} - 1.4std, 12H, 30T ; 16 velwindow", fontsize=16)
    
    # Northing plot
    ax1.scatter(df['ts'], df[northing_col], label='Northing Data Points', color='lightblue', alpha=0.5)
    ax1.plot(df['ts'], df['best_fit_northing'], label='Rolling Best Fit (Northing)', color='red', linewidth=2, marker='o', markersize=4)
    ax1.set_title('Northing Data Points and Rolling Best Fit')
    ax1.set_ylabel('Northing Difference (m)')
    ax1.legend()

    # Easting plot
    ax2.scatter(df['ts'], df[easting_col], label='Easting Data Points', color='lightgreen', alpha=0.5)
    ax2.plot(df['ts'], df['best_fit_easting'], label='Rolling Best Fit (Easting)', color='green', linewidth=2, marker='o', markersize=4)
    ax2.set_title('Easting Data Points and Rolling Best Fit')
    ax2.set_ylabel('Easting Difference (m)')
    ax2.legend()

    # Velocity plot
    ax3.plot(df['ts'], df['velocity_cm_hr'], label='Rolling Velocity (cm/hr)', color='purple', linewidth=2, marker='o', markersize=4)
    ax3.set_title('Rolling Velocity')
    ax3.set_xlabel('Timestamp')
    ax3.set_ylabel('Velocity (cm/hr)')
    ax3.legend()
    
    plt.tight_layout()
    plt.show()
    return df[['ts', 'northing_slope', 'easting_slope', 'velocity_cm_hr']]

def write_alert_ranges_to_db(rover_id, rover_name, df):
    connection = create_db_connection()
    if connection:
        alert_df = df[df['alert_level'] >= 2].copy()
        if not alert_df.empty:
            alert_df = alert_df.sort_values(by='ts').reset_index(drop=True)
            alert_df['time_diff'] = alert_df['ts'].diff().dt.total_seconds().div(60)  # Difference in minutes
            alert_df['group'] = (
                (alert_df['time_diff'] > 30) | (alert_df['alert_level'] != alert_df['alert_level'].shift())
            ).cumsum()

            alert_ranges = alert_df.groupby(['alert_level', 'group']).agg(
                ts_start=('ts', 'min'),
                ts_end=('ts', 'max')
            ).reset_index()

            try:
                for _, row in alert_ranges.iterrows():
                    query = f"""
                    INSERT INTO ublox_alerts (rover_id, ts_start, ts_end, alert_level)
                    VALUES ({rover_id}, '{row['ts_start']}', '{row['ts_end']}', {row['alert_level']})
                    ON DUPLICATE KEY UPDATE 
                        ts_start=VALUES(ts_start), 
                        ts_end=VALUES(ts_end), 
                        alert_level=VALUES(alert_level);
                    """
                    connection.execute(query)
            except Exception as e:
                print(f"Error writing alert ranges to DB: {e}")
            finally:
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
        rover_id, ref_lat, ref_lon = get_rover_reference_point(rover_name)
        ref_easting, ref_northing = convert_to_utm(ref_lon, ref_lat)

        dataframes = fetch_all_ts_data(table_name)
        df = dataframes
        df[['easting', 'northing']] = df.apply(lambda row: convert_to_utm(row['longitude'], row['latitude']), axis=1, result_type='expand')
        
        # Compute difference from the reference point
        df['easting_diff'] = df['easting'] - ref_easting
        df['northing_diff'] = df['northing'] - ref_northing
        df['distance_cm'] = df.apply(lambda row: euclidean_distance(row['easting'], row['northing'], ref_easting, ref_northing), axis=1)

        # Apply filters, resample, compute velocity and check alert
        df_filtered = apply_filters(df)
        df_filtered = resample_df(df_filtered).fillna(method='ffill')            
        df_velocity = compute_rolling_velocity(df_filtered, plot=False)
        df_alerts = check_alerts(df_velocity)

        write_alert_ranges_to_db(rover_id, rover_name, df_alerts)
        
if __name__ == "__main__":
    process_gnss_data()