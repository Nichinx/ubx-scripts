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

pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.max_column', None)

# db_config = {'host': 'localhost',
#             'user': 'root',
#             'password': 'admin123',
#             'database': 'new_schema_2'
# }
db_config = {'host':" 192.168.150.112",
            'database': "analysis_db",
            'user': "pysys_local",
            'password': "NaCAhztBgYZ3HwTkvHwwGVtJn5sVMFgg"
}

connection = mysql.connector.connect(**db_config)
# query = "SELECT * FROM analysis_db.gnss_tesua where ts between '2024-08-13 17:00' and '2024-08-14 13:00' order by ts"  #soaked ref point

###LAT
# query = "SELECT * FROM analysis_db.gnss_tesua where ts between '2024-08-15 08:00' and '2024-08-15 11:00' order by ts" #3cm point latitude
# query = "SELECT * FROM analysis_db.gnss_tesua where ts between '2024-08-15 13:00' and '2024-08-15 16:10' order by ts" #5cm point latitude
# query = "SELECT * FROM analysis_db.gnss_tesua where ts between '2024-08-15 16:20' and '2024-08-15 19:40' order by ts" #5cm point latitude (nadoble)
# query = "SELECT * FROM analysis_db.gnss_tesua where ts between '2024-08-16 15:00' and '2024-08-16 18:00' order by ts" #10cm point latitude

###LON
query = "SELECT * FROM analysis_db.gnss_tesua where ts between '2024-08-16 18:30' and '2024-08-16 21:30' order by ts" #3cm point longitude
# query = "SELECT * FROM analysis_db.gnss_tesua where ts between '2024-08-' and '2024-08-' order by ts" #5cm point longitude

  

df = pd.read_sql(query, connection)

# df = pd.read_csv("C:\\Users\\nichm\\Downloads\\tes.csv")
# df = df.sort_values(by='ts')


# Define the projection for WGS84 and UTM Zone 51N
wgs84 = Proj(proj='latlong', datum='WGS84')
utm_zone_51 = Proj(proj='utm', zone=51, datum='WGS84')
def convert_to_utm(lat, lon):
    easting, northing = transform(wgs84, utm_zone_51, lon, lat)
    return easting, northing #in meters


# from pyproj import Transformer
# transformer = Transformer.from_crs("epsg:4326", "epsg:32651", always_xy=True)
# def convert_to_utm(lat, lon):
#     easting, northing = transformer.transform(lon, lat)
#     return easting, northing


def euclidean_distance(x1, y1, x2, y2):
    distance = math.sqrt((x2 - x1)**2 + (y2 - y1)**2)
    distance_cm = distance * 100  # Convert to centimeters
    return distance_cm

def resample_df(df):
    df['ts'] = pd.to_datetime(df['ts'], unit='s')
    df = df.set_index('ts').resample('10min').mean().reset_index()
    return df

def prepare_and_apply_sanity_filters(df, hacc=0.0141, vacc=0.0141):
    # df['msl'] = pd.to_numeric(df['msl'], errors='coerce')
    df['msl'] = np.round(df['msl'], 3)
    df = df[(df['fix_type'] == 2) & (df['sat_num'] >= 20)]

    if df.empty:
        return df

    df = df[(df['hacc'] == hacc) & (df['vacc'] <= vacc)]
    df = df.reset_index(drop=True).sort_values(by='ts', ascending=True, ignore_index=True)

    return df

def outlier_filter_for_latlon_with_msl(df):
    df = df.copy()
    dfmean = df[['latitude', 'longitude', 'msl','distance_cm']].rolling(window=12, min_periods=1).mean()
    dfsd = df[['latitude', 'longitude', 'msl','distance_cm']].rolling(window=12, min_periods=1).std()

    dfulimits = dfmean + (2 * dfsd)  # 1 std
    dfllimits = dfmean - (2 * dfsd)

    df['latitude'] = df['latitude'].where((df['latitude'] <= dfulimits['latitude']) & (df['latitude'] >= dfllimits['latitude']), np.nan)
    df['longitude'] = df['longitude'].where((df['longitude'] <= dfulimits['longitude']) & (df['longitude'] >= dfllimits['longitude']), np.nan)
    df['msl'] = df['msl'].where((df['msl'] <= dfulimits['msl']) & (df['msl'] >= dfllimits['msl']), np.nan)
    df['distance_cm'] = df['distance_cm'].where((df['distance_cm'] <= dfulimits['distance_cm']) & (df['distance_cm'] >= dfllimits['distance_cm']), np.nan)

    df = df.dropna(subset=['latitude', 'longitude', 'msl','distance_cm'])

    return df

# Fixed coordinates - assumed rover position
# fixed_lat = 14.651944
# fixed_lon = 121.058402
# fixed_lat = 14.651941
# fixed_lon =  121.058402
# fixed_lat = 14.651945
# fixed_lon =  121.058404
# fixed_lat, fixed_lon = convert_to_utm(fixed_lat, fixed_lon)
# fixed_lat, fixed_lon = 290895.3488569876, 1620726.5405142051 #-> diko alam pano kinuha, anong timestamp range ito T_T
fixed_lat, fixed_lon = 290895.34846509795, 1620726.5411689582


df[['latitude', 'longitude']] = df.apply(
    lambda row: convert_to_utm(row['latitude'], row['longitude']), 
    axis=1, 
    result_type='expand'
)

# Calculate distances
df['distance_cm'] = df.apply(lambda row: euclidean_distance(row['latitude'], row['longitude'], fixed_lat, fixed_lon), axis=1)
print('df len = ', len(df))

# Convert 'ts' column to datetime
# df['ts'] = pd.to_datetime(df['ts'])
# df = resample_df(df).fillna(method='ffill')


def format_column(column):
    if column.name == 'ts':
        return column  # Skip formatting for 'ts' column
    elif column.name in ['data_id','fix_type', 'sat_num']:
        return column.map(lambda x: f"{x:.0f}")
    elif column.name in ['temp', 'volt']:
        return column.map(lambda x: f"{x:.2f}")
    elif column.name in ['hacc', 'vacc', 'msl']:
        return column.map(lambda x: f"{x:.4f}")
    else:
        # Default formatting for other columns with full precision
        return column.map(lambda x: f"{x:.9f}")

# Apply the formatter to each column in the dataframe
formatted_df = df.apply(format_column)
# df=formatted_df

# Define parameters
num_timestamps_ahead = 12
timestamp_freq = '10T'  # Frequency of timestamps in original data
rolling_window_size = num_timestamps_ahead # Adjust this based on your actual window size

# Calculate the starting timestamp and generate new timestamps if needed
start_ts = df['ts'].min()
end_ts = df['ts'].max()

# Get existing data points before the initial timestamp
existing_data_before = df[df['ts'] < start_ts].shape[0]

# Calculate how many timestamps are needed to fill the rolling window
num_needed_initial_ts = rolling_window_size - existing_data_before
num_needed_initial_ts = max(num_needed_initial_ts, 0)  # Ensure no negative values

if num_needed_initial_ts > 0:
    # Generate extra timestamps ahead of the initial timestamp if not enough data
    start_ts = df['ts'].min() - pd.Timedelta(timestamp_freq) * num_needed_initial_ts
    new_ts = pd.date_range(start=start_ts, periods=num_needed_initial_ts, freq=timestamp_freq)
    
    # Create a dataframe with new timestamps
    df_new_timestamps = pd.DataFrame({
        'ts': new_ts,
        'latitude': [np.nan] * len(new_ts),
        'longitude': [np.nan] * len(new_ts),
        'msl': [np.nan] * len(new_ts)
    })
    
    # Concatenate the new timestamps dataframe with the original dataframe
    df_extended = pd.concat([df_new_timestamps, df], ignore_index=True)
else:
    # If existing data is sufficient, use the original dataframe directly
    df_extended = df.copy()


df_filled = df_extended.fillna(method='bfill')
df_filtered = prepare_and_apply_sanity_filters(df_filled)
# print('df_filtered = ', len(df_filtered))
df_filtered = outlier_filter_for_latlon_with_msl(df_filtered)
# print('df_filtered = ', len(df_filtered))


df_result = df_filtered[df_filtered['ts'] >= df['ts'].min()]

df_filtered_orig_retained_length = df_result
print('df_filtered_orig_retained_length = ', len(df_filtered_orig_retained_length))
print('data percentage after filtering: ', np.round((len(df_filtered_orig_retained_length)/len(df))*100, 2))

df_result = resample_df(df_result).fillna(method='ffill')
# print('df_result = ', len(df_result))

# Ensure the end timestamp matches the original df's end timestamp
original_end_ts = df['ts'].max()
result_end_ts = df_result['ts'].max()

if result_end_ts < original_end_ts:
    # Add a row with the original end timestamp if it's missing
    end_row = df_result.iloc[-1].copy()
    end_row['ts'] = original_end_ts
    df_result = pd.concat([df_result, pd.DataFrame([end_row])], ignore_index=True)



# Forward-fill again to ensure data continuity
df_result = resample_df(df_result).fillna(method='ffill')
print('df_result = ', len(df_result))


connection.close()