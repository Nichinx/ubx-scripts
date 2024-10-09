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
            'user': "hardwareinfra",
            'password': "veug3r4MTKfsuk5H4rdw4r3"
}

connection = mysql.connector.connect(**db_config)
# query = "SELECT * FROM analysis_db.gnss_tesua where ts between '2024-08-13 17:00' and '2024-08-14 13:00' order by ts"  #soaked ref point
# query = "SELECT * FROM analysis_db.gnss_tesua where ts > '2024-08-29 14:20' order by ts"  #soak for elev



###LAT
# query = "SELECT * FROM analysis_db.gnss_tesua where ts between '2024-08-15 08:00' and '2024-08-15 11:00' order by ts" #3cm point latitude
# query = "SELECT * FROM analysis_db.gnss_tesua where ts between '2024-08-15 13:00' and '2024-08-15 16:10' order by ts" #5cm point latitude
# query = "SELECT * FROM analysis_db.gnss_tesua where ts between '2024-08-15 16:20' and '2024-08-15 19:40' order by ts" #5cm point latitude (nadoble)
# query = "SELECT * FROM analysis_db.gnss_tesua where ts between '2024-08-16 15:00' and '2024-08-16 18:00' order by ts" #10cm point latitude

###LON
# query = "SELECT * FROM analysis_db.gnss_tesua where ts between '2024-08-16 18:30' and '2024-08-16 21:30' order by ts" #3cm point longitude
# query = "SELECT * FROM analysis_db.gnss_tesua where ts between '2024-08-20 16:00' and '2024-08-20 19:00' order by ts" #5cm point longitude
# query = "SELECT * FROM analysis_db.gnss_tesua where ts between '2024-08-20 22:30' and '2024-08-21 01:30' order by ts" #5cm point longitude (inulit)
# query = "SELECT * FROM analysis_db.gnss_tesua where ts between '2024-08-20 09:00' and '2024-08-20 12:00' order by ts" #10cm point longitude (nauna. hay)

###DIAG
# query = "SELECT * FROM analysis_db.gnss_tesua where ts between '2024-08-20 12:30' and '2024-08-20 15:30' order by ts" #3cm sq diag xy
# query = "SELECT * FROM analysis_db.gnss_tesua where ts between '2024-08-21 6:30' and '2024-08-21 09:30' order by ts" #5cm sq diag xy
# query = "SELECT * FROM analysis_db.gnss_tesua where ts between '2024-08-22 13:30' and '2024-08-22 16:30' order by ts" #10cm sq diag xy


# ###ELEV/MSL
# query = "SELECT * FROM analysis_db.gnss_tesua where ts between '2024-08-22 17:30' and '2024-08-22 20:40' order by ts" #3xm z axis



# df = pd.read_sql(query, connection)

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
    # dfmean = df[['latitude', 'longitude', 'msl','distance_cm']].rolling(window=12, min_periods=1).mean()
    # dfsd = df[['latitude', 'longitude', 'msl','distance_cm']].rolling(window=12, min_periods=1).std()

    dfmean = df[['latitude', 'longitude', 'msl']].rolling(window=12, min_periods=1).mean()
    dfsd = df[['latitude', 'longitude', 'msl']].rolling(window=12, min_periods=1).std()


    dfulimits = dfmean + (2 * dfsd)  # 1 std
    dfllimits = dfmean - (2 * dfsd)

    df['latitude'] = df['latitude'].where((df['latitude'] <= dfulimits['latitude']) & (df['latitude'] >= dfllimits['latitude']), np.nan)
    df['longitude'] = df['longitude'].where((df['longitude'] <= dfulimits['longitude']) & (df['longitude'] >= dfllimits['longitude']), np.nan)
    df['msl'] = df['msl'].where((df['msl'] <= dfulimits['msl']) & (df['msl'] >= dfllimits['msl']), np.nan)
    # df['distance_cm'] = df['distance_cm'].where((df['distance_cm'] <= dfulimits['distance_cm']) & (df['distance_cm'] >= dfllimits['distance_cm']), np.nan)

    # df = df.dropna(subset=['latitude', 'longitude', 'msl','distance_cm'])
    df = df.dropna(subset=['latitude', 'longitude', 'msl'])

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


# df[['latitude', 'longitude']] = df.apply(
#     lambda row: convert_to_utm(row['latitude'], row['longitude']), 
#     axis=1, 
#     result_type='expand'
# )

# Calculate distances
# df['distance_cm'] = df.apply(lambda row: euclidean_distance(row['latitude'], row['longitude'], fixed_lat, fixed_lon), axis=1)
# print('df len = ', len(df))

# start_ts = pd.to_datetime(df['ts'].min())
# end_ts = pd.to_datetime(df['ts'].max())

#Convert 'ts' column to datetime
# df['ts'] = pd.tdfo_datetime(df['ts'])
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
# formatted_df = df.apply(format_column)
# df=formatted_df




# # Define parameters
# num_timestamps_ahead = 12
# timestamp_freq = '10T'  # Frequency of timestamps in original data
# rolling_window_size = num_timestamps_ahead # Adjust this based on your actual window size

# # Calculate the starting timestamp and generate new timestamps if needed
# start_ts = df['ts'].min()
# end_ts = df['ts'].max()

# # Get existing data points before the initial timestamp
# existing_data_before = df[df['ts'] < start_ts].shape[0]

# # Calculate how many timestamps are needed to fill the rolling window
# num_needed_initial_ts = rolling_window_size - existing_data_before
# num_needed_initial_ts = max(num_needed_initial_ts, 0)  # Ensure no negative values

# if num_needed_initial_ts > 0:
#     # Generate extra timestamps ahead of the initial timestamp if not enough data
#     start_ts = df['ts'].min() - pd.Timedelta(timestamp_freq) * num_needed_initial_ts
#     new_ts = pd.date_range(start=start_ts, periods=num_needed_initial_ts, freq=timestamp_freq)
    
#     # Create a dataframe with new timestamps
#     df_new_timestamps = pd.DataFrame({
#         'ts': new_ts,
#         'latitude': [np.nan] * len(new_ts),
#         'longitude': [np.nan] * len(new_ts),
#         'msl': [np.nan] * len(new_ts)
#     })
    
#     # Concatenate the new timestamps dataframe with the original dataframe
#     df_extended = pd.concat([df_new_timestamps, df], ignore_index=True)
# else:
#     # If existing data is sufficient, use the original dataframe directly
#     df_extended = df.copy()


# df_filled = df_extended.fillna(method='bfill')
# df_filtered = prepare_and_apply_sanity_filters(df_filled)
# # print('df_filtered = ', len(df_filtered))
# df_filtered = outlier_filter_for_latlon_with_msl(df_filtered)
# # print('df_filtered = ', len(df_filtered))


# df_result = df_filtered[df_filtered['ts'] >= df['ts'].min()]

# df_filtered_orig_retained_length = df_result
# print('df_filtered_orig_retained_length = ', len(df_filtered_orig_retained_length))
# print('data percentage after filtering: ', np.round((len(df_filtered_orig_retained_length)/len(df))*100, 2))

# df_result = resample_df(df_result).fillna(method='ffill')
# # print('df_result = ', len(df_result))

# # Ensure the end timestamp matches the original df's end timestamp
# original_end_ts = df['ts'].max()
# result_end_ts = df_result['ts'].max()

# if result_end_ts < original_end_ts:
#     # Add a row with the original end timestamp if it's missing
#     end_row = df_result.iloc[-1].copy()
#     end_row['ts'] = original_end_ts
#     df_result = pd.concat([df_result, pd.DataFrame([end_row])], ignore_index=True)



# # Forward-fill again to ensure data continuity
# df_result = resample_df(df_result).fillna(method='ffill')
# print('df_result = ', len(df_result))







# # Calculate the offset start time based on rolling window size
# rolling_window_size = 12  # Adjust based on your actual window size
# offset_hours = (rolling_window_size * 10) / 60  # Convert rolling window size to hours (10 min intervals)
# offset_start_ts = start_ts - timedelta(hours=offset_hours)  # Adjust to your needs

# # Query data with offset start time
# query_with_offset = f"""
#     SELECT * FROM analysis_db.gnss_tesua
#     WHERE ts BETWEEN '{offset_start_ts}' AND '{end_ts}' 
#     ORDER BY ts
# """
# df_offset = pd.read_sql(query_with_offset, connection)

# # Apply UTM conversion
# df_offset[['latitude', 'longitude']] = df_offset.apply(
#     lambda row: convert_to_utm(row['latitude'], row['longitude']), 
#     axis=1, result_type='expand'
# )

# # Calculate distance from fixed point
# df_offset['distance_cm'] = df_offset.apply(
#     lambda row: euclidean_distance(row['latitude'], row['longitude'], fixed_lat, fixed_lon), axis=1
# )

# # Apply sanity filters and outlier filtering
# df_offset = prepare_and_apply_sanity_filters(df_offset)
# df_filtered = outlier_filter_for_latlon_with_msl(df_offset)
# # print('df_filtered = ', len(df_filtered))

# df_filtered_orig_retained_length = df_filtered[df_filtered['ts'] >= df['ts'].min()]
# print('df_filtered_orig_retained_length = ', len(df_filtered_orig_retained_length))
# print('data percentage after filtering: ', np.round((len(df_filtered_orig_retained_length)/len(df))*100, 2))

# # Trim data back to the original start and end times dynamically
# df_result = df_filtered[(df_filtered['ts'] >= start_ts) & (df_filtered['ts'] <= end_ts)]

# # Resample and forward-fill the final dataframe
# df_result = resample_df(df_result).fillna(method='ffill')

# print(f'Final dataframe length after filtering: {len(df_result)}')
# # print(f'Data percentage after filtering: {np.round((len(df_result)/len(df_offset))*100, 2)}%')





############################################################################## MSL TEST
# # query1 = "SELECT * FROM analysis_db.gnss_tesua where ts between '2024-08-29 14:20' and '2024-08-29 16:50' order by ts" #ref.point resoak
# query1 = "SELECT * FROM analysis_db.gnss_tesua where ts between '2024-08-31 13:00' and '2024-08-31 18:00' order by ts" #ref.point resoak 2

# query2 = "SELECT * FROM analysis_db.gnss_tesua where ts between '2024-08-29 17:00' and '2024-08-29 20:00' order by ts" #3xm z axis
# query2 = "SELECT * FROM analysis_db.gnss_tesua where ts between '2024-08-30 11:40' and '2024-08-30 14:40' order by ts" #5xm z axis
# query2 = "SELECT * FROM analysis_db.gnss_tesua where ts between '2024-08-30 15:00' and '2024-08-30 18:00' order by ts" #8xm z axis
# query2 = "SELECT * FROM analysis_db.gnss_tesua where ts between '2024-08-31 09:00' and '2024-08-31 12:00' order by ts" #10xm z axis


# # Fetch data from query1 and query2
# df_query1 = pd.read_sql(query1, connection)
# df_query2 = pd.read_sql(query2, connection)


# df_query1[['latitude', 'longitude']] = df_query1.apply(
#     lambda row: convert_to_utm(row['latitude'], row['longitude']), 
#     axis=1, 
#     result_type='expand'
# )
# mean_msl_query1 = df_query1['msl'].mean()
# # df_query1['msl_difference'] = (df_query1['msl'] - mean_msl_query1) * 100  # Convert to centimeters
# # df_query1['distance_cm'] = df_query1['msl_difference']

# num_timestamps_ahead = 12
# timestamp_freq = '10T'  # Frequency of timestamps in original data
# rolling_window_size = num_timestamps_ahead # Adjust this based on your actual window size

# start_ts_query1_orig = df_query1['ts'].min()
# end_ts_query1 = df_query1['ts'].max()
# existing_data_before_query1 = df_query1[df_query1['ts'] < start_ts_query1_orig].shape[0]
# num_needed_initial_ts_query1 = rolling_window_size - existing_data_before_query1
# num_needed_initial_ts_query1 = max(num_needed_initial_ts_query1, 0)

# if num_needed_initial_ts_query1 > 0:
#     start_ts_query1 = df_query1['ts'].min() - pd.Timedelta(timestamp_freq) * num_needed_initial_ts_query1
#     new_ts_query1 = pd.date_range(start=start_ts_query1, periods=num_needed_initial_ts_query1, freq=timestamp_freq)
    
#     df_new_timestamps_query1 = pd.DataFrame({
#         'ts': new_ts_query1,
#         'latitude': [np.nan] * len(new_ts_query1),
#         'longitude': [np.nan] * len(new_ts_query1),
#         'msl': [np.nan] * len(new_ts_query1)
#     })
    
#     df_query1_extended = pd.concat([df_new_timestamps_query1, df_query1], ignore_index=True)
# else:
#     df_query1_extended = df_query1.copy()

# df_query1_filled = df_query1_extended.fillna(method='bfill')
# df_query1_filtered = prepare_and_apply_sanity_filters(df_query1_filled)
# df_query1_filtered = outlier_filter_for_latlon_with_msl(df_query1_filtered)

# # df_query1_result = resample_df(df_query1_filtered).fillna(method='ffill')

# original_end_ts_query1 = df_query1['ts'].max()
# result_end_ts_query1 = df_query1_filtered['ts'].max()

# if result_end_ts_query1 < original_end_ts_query1:
#     end_row_query1 = df_query1_filtered.iloc[-1].copy()
#     end_row_query1['ts'] = original_end_ts_query1
#     df_query1_result = pd.concat([df_query1_filtered, pd.DataFrame([end_row_query1])], ignore_index=True)
#     df_query1_filtered = df_query1_filtered[(df_query1_filtered['ts'] >= start_ts_query1_orig) & (df_query1_filtered['ts'] <= end_ts_query1)]
# else:
#     df_query1_filtered = df_query1_filtered[(df_query1_filtered['ts'] >= start_ts_query1_orig) & (df_query1_filtered['ts'] <= end_ts_query1)]



# df = df_query2
# print("df = ", len(df))

# df[['latitude', 'longitude']] = df.apply(
#     lambda row: convert_to_utm(row['latitude'], row['longitude']), 
#     axis=1, 
#     result_type='expand'
# )

# start_ts = pd.to_datetime(df['ts'].min())
# end_ts = pd.to_datetime(df['ts'].max())

# # Step 1: Calculate the mean MSL from query1
# df_query1 = df_query1_filtered
# mean_msl_query1 = df_query1['msl'].mean()

# # Step 2: Calculate the MSL difference for query2
# df['msl_difference'] = (df['msl'] - mean_msl_query1) * 100  # Convert to centimeters

# # Step 3: Replace the 'distance_cm' column with 'msl_difference'
# df['distance_cm'] = df['msl_difference']



# num_timestamps_ahead = 12
# timestamp_freq = '10T'  # Frequency of timestamps in original data
# rolling_window_size = num_timestamps_ahead # Adjust this based on your actual window size

# # Calculate the starting timestamp and generate new timestamps if needed
# start_ts = df['ts'].min()
# end_ts = df['ts'].max()

# # Get existing data points before the initial timestamp
# existing_data_before = df[df['ts'] < start_ts].shape[0]

# # Calculate how many timestamps are needed to fill the rolling window
# num_needed_initial_ts = rolling_window_size - existing_data_before
# num_needed_initial_ts = max(num_needed_initial_ts, 0)  # Ensure no negative values

# if num_needed_initial_ts > 0:
#     # Generate extra timestamps ahead of the initial timestamp if not enough data
#     start_ts = df['ts'].min() - pd.Timedelta(timestamp_freq) * num_needed_initial_ts
#     new_ts = pd.date_range(start=start_ts, periods=num_needed_initial_ts, freq=timestamp_freq)
    
#     # Create a dataframe with new timestamps
#     df_new_timestamps = pd.DataFrame({
#         'ts': new_ts,
#         'latitude': [np.nan] * len(new_ts),
#         'longitude': [np.nan] * len(new_ts),
#         'msl': [np.nan] * len(new_ts)
#     })
    
#     # Concatenate the new timestamps dataframe with the original dataframe
#     df_extended = pd.concat([df_new_timestamps, df], ignore_index=True)
# else:
#     # If existing data is sufficient, use the original dataframe directly
#     df_extended = df.copy()


# df_filled = df_extended.fillna(method='bfill')
# df_filtered = prepare_and_apply_sanity_filters(df_filled)
# df_filtered = outlier_filter_for_latlon_with_msl(df_filtered)


# df_result = df_filtered[df_filtered['ts'] >= df['ts'].min()]

# df_filtered_orig_retained_length = df_result
# print('df_filtered_orig_retained_length = ', len(df_filtered_orig_retained_length))
# print('data percentage after filtering: ', np.round((len(df_filtered_orig_retained_length)/len(df))*100, 2))

# df_result = resample_df(df_result).fillna(method='ffill')
# # print('df_result = ', len(df_result))

# # Ensure the end timestamp matches the original df's end timestamp
# original_end_ts = df['ts'].max()
# result_end_ts = df_result['ts'].max()

# if result_end_ts < original_end_ts:
#     # Add a row with the original end timestamp if it's missing
#     end_row = df_result.iloc[-1].copy()
#     end_row['ts'] = original_end_ts
#     df_result = pd.concat([df_result, pd.DataFrame([end_row])], ignore_index=True)


# # Forward-fill again to ensure data continuity
# df_result = resample_df(df_result).fillna(method='ffill')
# print('df_result = ', len(df_result))




###################
# # List of queries and query names
# queries = {
#     'ref' : "SELECT * FROM analysis_db.gnss_tesua where ts between '2024-08-13 17:00' and '2024-08-14 13:00' order by ts",
#     '3cm_lat': "SELECT * FROM analysis_db.gnss_tesua where ts between '2024-08-15 08:00' and '2024-08-15 11:00' order by ts",
#     '5cm_lat': "SELECT * FROM analysis_db.gnss_tesua where ts between '2024-08-15 13:00' and '2024-08-15 16:10' order by ts",
#     '10cm_lat': "SELECT * FROM analysis_db.gnss_tesua where ts between '2024-08-16 15:00' and '2024-08-16 18:00' order by ts",
#     '3cm_lon': "SELECT * FROM analysis_db.gnss_tesua where ts between '2024-08-16 18:30' and '2024-08-16 21:30' order by ts",
#     '5cm_lon': "SELECT * FROM analysis_db.gnss_tesua where ts between '2024-08-20 22:30' and '2024-08-21 01:30' order by ts",
#     '10cm_lon': "SELECT * FROM analysis_db.gnss_tesua where ts between '2024-08-20 09:00' and '2024-08-20 12:00' order by ts",
#     '3cm_diag': "SELECT * FROM analysis_db.gnss_tesua where ts between '2024-08-20 12:30' and '2024-08-20 15:30' order by ts",
#     '5cm_diag': "SELECT * FROM analysis_db.gnss_tesua where ts between '2024-08-21 6:30' and '2024-08-21 09:30' order by ts",
#     '10cm_diag': "SELECT * FROM analysis_db.gnss_tesua where ts between '2024-08-22 13:30' and '2024-08-22 16:30' order by ts"
# }

# # Process each query
# for query_name, query in queries.items():
#     # Fetch data
#     df_raw = pd.read_sql(query, connection)
    
#     # Convert lat/lon to UTM
#     df_raw[['latitude', 'longitude']] = df_raw.apply(
#         lambda row: convert_to_utm(row['latitude'], row['longitude']), 
#         axis=1, 
#         result_type='expand'
#     )
    
#     # Fixed reference point for distance calculation
#     fixed_lat, fixed_lon = 290895.34846509795, 1620726.5411689582
#     df_raw['distance_cm'] = df_raw.apply(lambda row: euclidean_distance(row['latitude'], row['longitude'], fixed_lat, fixed_lon), axis=1)
    
#     # Fill timestamps if needed
#     start_ts = df_raw['ts'].min()
#     end_ts = df_raw['ts'].max()
#     num_timestamps_ahead = 12
#     timestamp_freq = '10T'
#     rolling_window_size = num_timestamps_ahead
    
#     existing_data_before = df_raw[df_raw['ts'] < start_ts].shape[0]
#     num_needed_initial_ts = max(rolling_window_size - existing_data_before, 0)
    
#     if num_needed_initial_ts > 0:
#         start_ts = df_raw['ts'].min() - pd.Timedelta(timestamp_freq) * num_needed_initial_ts
#         new_ts = pd.date_range(start=start_ts, periods=num_needed_initial_ts, freq=timestamp_freq)
#         df_new_timestamps = pd.DataFrame({
#             'ts': new_ts,
#             'latitude': [np.nan] * len(new_ts),
#             'longitude': [np.nan] * len(new_ts),
#             'msl': [np.nan] * len(new_ts)
#         })
#         df_raw_extended = pd.concat([df_new_timestamps, df_raw], ignore_index=True)
#     else:
#         df_raw_extended = df_raw.copy()

#     # Forward-fill missing data
#     df_raw_filled = df_raw_extended.fillna(method='bfill')
    
#     # Apply filters (custom functions)
#     df_filtered = prepare_and_apply_sanity_filters(df_raw_filled)
#     df_filtered = outlier_filter_for_latlon_with_msl(df_filtered)
    
#     # Resample and forward-fill
#     df_result = resample_df(df_filtered).fillna(method='ffill')

#     # Ensure the end timestamp matches
#     original_end_ts = df_raw['ts'].max()
#     if df_result['ts'].max() < original_end_ts:
#         end_row = df_result.iloc[-1].copy()
#         end_row['ts'] = original_end_ts
#         df_result = pd.concat([df_result, pd.DataFrame([end_row])], ignore_index=True)
    
#     df_result = resample_df(df_result).fillna(method='ffill')

#     # Export raw and filtered data to CSV
#     df_raw.to_csv(f'df_raw_{query_name}.csv', index=False)
#     df_result.to_csv(f'df_filtered_{query_name}.csv', index=False)




###################### export list of queries to csv:
queries = {
    # 'ref' : "SELECT * FROM analysis_db.gnss_tesua where ts between '2024-08-13 17:00' and '2024-08-14 13:00' order by ts",
    '3cm_lat': "SELECT * FROM analysis_db.gnss_tesua where ts between '2024-08-15 08:00' and '2024-08-15 11:00' order by ts",
    '5cm_lat': "SELECT * FROM analysis_db.gnss_tesua where ts between '2024-08-15 13:00' and '2024-08-15 16:10' order by ts",
    '10cm_lat': "SELECT * FROM analysis_db.gnss_tesua where ts between '2024-08-16 15:00' and '2024-08-16 18:00' order by ts",
    '3cm_lon': "SELECT * FROM analysis_db.gnss_tesua where ts between '2024-08-16 18:30' and '2024-08-16 21:30' order by ts",
    '5cm_lon': "SELECT * FROM analysis_db.gnss_tesua where ts between '2024-08-20 22:30' and '2024-08-21 01:30' order by ts",
    '10cm_lon': "SELECT * FROM analysis_db.gnss_tesua where ts between '2024-08-20 09:00' and '2024-08-20 12:00' order by ts",
    '3cm_diag': "SELECT * FROM analysis_db.gnss_tesua where ts between '2024-08-20 12:30' and '2024-08-20 15:30' order by ts",
    '5cm_diag': "SELECT * FROM analysis_db.gnss_tesua where ts between '2024-08-21 6:30' and '2024-08-21 09:30' order by ts",
    '10cm_diag': "SELECT * FROM analysis_db.gnss_tesua where ts between '2024-08-22 13:30' and '2024-08-22 16:30' order by ts"
}

# Process each query
for query_name, query in queries.items():
    # Fetch data
    df_raw = pd.read_sql(query, connection)
    
    # Convert lat/lon to UTM
    df_raw[['latitude_utm', 'longitude_utm']] = df_raw.apply(
        lambda row: convert_to_utm(row['latitude'], row['longitude']), 
        axis=1, 
        result_type='expand'
    )
    
    # Fixed reference point for distance calculation
    fixed_lat, fixed_lon = 290895.34846509795, 1620726.5411689582
    df_raw['distance_cm'] = df_raw.apply(lambda row: euclidean_distance(row['latitude_utm'], row['longitude_utm'], fixed_lat, fixed_lon), axis=1)
    
    print('df_raw len: ', len(df_raw))
    print(df_raw)
    
    # Fill timestamps if needed
    num_timestamps_ahead = 12
    timestamp_freq = '10T'
    rolling_window_size = num_timestamps_ahead

    start_ts = df_raw['ts'].min()
    end_ts = df_raw['ts'].max()

    # Calculate the existing data before start_ts
    existing_data_before = df_raw[df_raw['ts'] < start_ts].shape[0]
    
    # Calculate how many timestamps we need before the initial timestamp
    num_needed_initial_ts = max(rolling_window_size - existing_data_before, 0)

    if num_needed_initial_ts > 0:
        start_ts = df_raw['ts'].min() - pd.Timedelta(timestamp_freq) * num_needed_initial_ts
        new_ts = pd.date_range(start=start_ts, periods=num_needed_initial_ts, freq=timestamp_freq)
        df_new_timestamps = pd.DataFrame({
            'ts': new_ts,
            'latitude': [np.nan] * len(new_ts),
            'longitude': [np.nan] * len(new_ts),
            'msl': [np.nan] * len(new_ts)
        })
        df_raw_extended = pd.concat([df_new_timestamps, df_raw], ignore_index=True)
    else:
        df_raw_extended = df_raw.copy()

    # Backward-fill missing data
    df_raw_filled = df_raw_extended.fillna(method='bfill')
    
    # Apply filters (custom functions)
    df_filtered = prepare_and_apply_sanity_filters(df_raw_filled)
    df_filtered = outlier_filter_for_latlon_with_msl(df_filtered)
    
    # Keep only data after the original minimum timestamp
    df_filtered = df_filtered[df_filtered['ts'] >= df_raw['ts'].min()]
    df_filtered_orig_retained_length = df_filtered
    print('df_filtered_orig_retained_length = ', len(df_filtered_orig_retained_length))
    print('data percentage after filtering: ', np.round((len(df_filtered_orig_retained_length)/len(df_raw))*100, 2))

    # Resample and forward-fill
    df_result = resample_df(df_filtered).fillna(method='ffill')

    # Ensure the end timestamp matches
    original_end_ts = df_raw['ts'].max()
    result_end_ts = df_result['ts'].max()

    if result_end_ts < original_end_ts:
        # Add a row with the original end timestamp if it's missing
        end_row = df_result.iloc[-1].copy()
        end_row['ts'] = original_end_ts
        df_result = pd.concat([df_result, pd.DataFrame([end_row])], ignore_index=True)

    # Forward-fill again to ensure data continuity
    df_result = resample_df(df_result).fillna(method='ffill')
    print('df_result len: ', len(df_result))
    print(df_result)
    print('* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *')

    # Export raw and filtered data to CSV
    # df_raw.to_csv(f'df_raw_{query_name}.csv', index=False)
    # df_result.to_csv(f'df_filtered_{query_name}.csv', index=False)

    df_result = df_result[['latitude','longitude']]
    df_result.to_csv(f'df_filtered_{query_name}.csv', index=False)


##############################################################################
##MSL queries to csv


# query1 = "SELECT * FROM analysis_db.gnss_tesua where ts between '2024-08-31 13:00' and '2024-08-31 18:00' order by ts" #ref.point resoak 2
# df_query1 = pd.read_sql(query1, connection)


# df_query1[['latitude', 'longitude']] = df_query1.apply(
#     lambda row: convert_to_utm(row['latitude'], row['longitude']), 
#     axis=1, 
#     result_type='expand'
# )
# mean_msl_query1 = df_query1['msl'].mean()
# df_query1['msl_difference'] = (df_query1['msl'] - mean_msl_query1) * 100  # Convert to centimeters
# df_query1['distance_cm'] = df_query1['msl_difference']

# num_timestamps_ahead = 12
# timestamp_freq = '10T'  # Frequency of timestamps in original data
# rolling_window_size = num_timestamps_ahead # Adjust this based on your actual window size

# start_ts_query1_orig = df_query1['ts'].min()
# end_ts_query1 = df_query1['ts'].max()
# existing_data_before_query1 = df_query1[df_query1['ts'] < start_ts_query1_orig].shape[0]
# num_needed_initial_ts_query1 = rolling_window_size - existing_data_before_query1
# num_needed_initial_ts_query1 = max(num_needed_initial_ts_query1, 0)

# if num_needed_initial_ts_query1 > 0:
#     start_ts_query1 = df_query1['ts'].min() - pd.Timedelta(timestamp_freq) * num_needed_initial_ts_query1
#     new_ts_query1 = pd.date_range(start=start_ts_query1, periods=num_needed_initial_ts_query1, freq=timestamp_freq)
    
#     df_new_timestamps_query1 = pd.DataFrame({
#         'ts': new_ts_query1,
#         'latitude': [np.nan] * len(new_ts_query1),
#         'longitude': [np.nan] * len(new_ts_query1),
#         'msl': [np.nan] * len(new_ts_query1)
#     })
    
#     df_query1_extended = pd.concat([df_new_timestamps_query1, df_query1], ignore_index=True)
# else:
#     df_query1_extended = df_query1.copy()

# df_query1_filled = df_query1_extended.fillna(method='bfill')
# df_query1_filtered = prepare_and_apply_sanity_filters(df_query1_filled)
# df_query1_filtered = outlier_filter_for_latlon_with_msl(df_query1_filtered)

# # df_query1_result = resample_df(df_query1_filtered).fillna(method='ffill')

# original_end_ts_query1 = df_query1['ts'].max()
# result_end_ts_query1 = df_query1_filtered['ts'].max()

# if result_end_ts_query1 < original_end_ts_query1:
#     end_row_query1 = df_query1_filtered.iloc[-1].copy()
#     end_row_query1['ts'] = original_end_ts_query1
#     df_query1_result = pd.concat([df_query1_filtered, pd.DataFrame([end_row_query1])], ignore_index=True)
#     df_query1_filtered = df_query1_filtered[(df_query1_filtered['ts'] >= start_ts_query1_orig) & (df_query1_filtered['ts'] <= end_ts_query1)]
# else:
#     df_query1_filtered = df_query1_filtered[(df_query1_filtered['ts'] >= start_ts_query1_orig) & (df_query1_filtered['ts'] <= end_ts_query1)]


# df_query1 = df_query1_filtered
# mean_msl_query1 = df_query1['msl'].mean()


# queries = {
#     'query2_3xm': "SELECT * FROM analysis_db.gnss_tesua where ts between '2024-08-29 17:00' and '2024-08-29 20:00' order by ts",
#     'query2_5xm': "SELECT * FROM analysis_db.gnss_tesua where ts between '2024-08-30 11:40' and '2024-08-30 14:40' order by ts",
#     'query2_8xm': "SELECT * FROM analysis_db.gnss_tesua where ts between '2024-08-30 15:00' and '2024-08-30 18:00' order by ts",
#     'query2_10xm': "SELECT * FROM analysis_db.gnss_tesua where ts between '2024-08-31 09:00' and '2024-08-31 12:00' order by ts"
# }



# for query_name, query in queries.items():
#     df_raw = pd.read_sql(query, connection)
    
#     # Convert lat/lon to UTM
#     df_raw[['latitude_utm', 'longitude_utm']] = df_raw.apply(
#         lambda row: convert_to_utm(row['latitude'], row['longitude']), 
#         axis=1, 
#         result_type='expand'
#     )
    
#     df_raw['msl_difference'] = (df_raw['msl'] - mean_msl_query1) * 100  # Convert to centimeters
#     df_raw['distance_cm'] = df_raw['msl_difference']
    
#     # Fill timestamps if needed
#     num_timestamps_ahead = 12
#     timestamp_freq = '10T'
#     rolling_window_size = num_timestamps_ahead

#     start_ts = df_raw['ts'].min()
#     end_ts = df_raw['ts'].max()

#     existing_data_before = df_raw[df_raw['ts'] < start_ts].shape[0]
#     num_needed_initial_ts = max(rolling_window_size - existing_data_before, 0)

#     if num_needed_initial_ts > 0:
#         start_ts = df_raw['ts'].min() - pd.Timedelta(timestamp_freq) * num_needed_initial_ts
#         new_ts = pd.date_range(start=start_ts, periods=num_needed_initial_ts, freq=timestamp_freq)
#         df_new_timestamps = pd.DataFrame({
#             'ts': new_ts,
#             'latitude': [np.nan] * len(new_ts),
#             'longitude': [np.nan] * len(new_ts),
#             'msl': [np.nan] * len(new_ts)
#         })
#         df_raw_extended = pd.concat([df_new_timestamps, df_raw], ignore_index=True)
#     else:
#         df_raw_extended = df_raw.copy()

#     df_raw_filled = df_raw_extended.fillna(method='bfill')

#     # Apply sanity filters and outlier removal
#     df_filtered = prepare_and_apply_sanity_filters(df_raw_filled)
#     df_filtered = outlier_filter_for_latlon_with_msl(df_filtered)
#     df_result = df_filtered[df_filtered['ts'] >= df_raw['ts'].min()]

#     # Add missing end timestamp if needed
#     original_end_ts = df_raw['ts'].max()
#     result_end_ts = df_result['ts'].max()
#     if result_end_ts < original_end_ts:
#         end_row = df_result.iloc[-1].copy()
#         end_row['ts'] = original_end_ts
#         df_result = pd.concat([df_result, pd.DataFrame([end_row])], ignore_index=True)

#     # Forward-fill again to ensure data continuity
#     df_result = resample_df(df_result).fillna(method='ffill')

#     # Export raw and filtered data to CSV
#     df_raw.to_csv(f'df_raw_{query_name}.csv', index=False)
#     df_result.to_csv(f'df_filtered_{query_name}.csv', index=False)




connection.close()