# -*- coding: utf-8 -*-
"""
Created on Fri Jun  7 22:44:23 2024

@author: nichm
"""

import mysql.connector
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import math
from matplotlib.ticker import MaxNLocator
import matplotlib.dates as mdates
from matplotlib.ticker import ScalarFormatter


def euclidean_distance(lat1, lon1, lat2, lon2):
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)

    R = 6371000  # Radius of the Earth in meters

    delta_lat = lat2_rad - lat1_rad
    delta_lon = lon2_rad - lon1_rad

    distance = math.sqrt((delta_lat * R)**2 + (delta_lon * R * math.cos((lat1_rad + lat2_rad) / 2))**2)
    distance_cm = distance * 100

    return distance_cm

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

def hybrid_outlier_filter_for_latlon_with_msl(df, rolling_window=20, rolling_factor=2, global_factor=3):
    df = df.copy()
    
    # Rolling window-based filtering
    dfmean = df[['latitude', 'longitude', 'msl']].rolling(window=rolling_window, min_periods=1).mean()
    dfsd = df[['latitude', 'longitude', 'msl']].rolling(window=rolling_window, min_periods=1).std()

    dfulimits = dfmean + (rolling_factor * dfsd)
    dfllimits = dfmean - (rolling_factor * dfsd)

    df['latitude'] = df['latitude'].where((df['latitude'] <= dfulimits['latitude']) & (df['latitude'] >= dfllimits['latitude']), np.nan)
    df['longitude'] = df['longitude'].where((df['longitude'] <= dfulimits['longitude']) & (df['longitude'] >= dfllimits['longitude']), np.nan)
    df['msl'] = df['msl'].where((df['msl'] <= dfulimits['msl']) & (df['msl'] >= dfllimits['msl']), np.nan)

    # Drop rows with NaN values after rolling window filtering
    df = df.dropna(subset=['latitude', 'longitude', 'msl'])
    
    # Global limit-based filtering
    latitude_upper = df['latitude'].mean() + global_factor * df['latitude'].std()
    latitude_lower = df['latitude'].mean() - global_factor * df['latitude'].std()
    longitude_upper = df['longitude'].mean() + global_factor * df['longitude'].std()
    longitude_lower = df['longitude'].mean() - global_factor * df['longitude'].std()
    msl_upper = df['msl'].mean() + global_factor * df['msl'].std()
    msl_lower = df['msl'].mean() - global_factor * df['msl'].std()

    df['latitude'] = df['latitude'].where((df['latitude'] <= latitude_upper) & (df['latitude'] >= latitude_lower), np.nan)
    df['longitude'] = df['longitude'].where((df['longitude'] <= longitude_upper) & (df['longitude'] >= longitude_lower), np.nan)
    df['msl'] = df['msl'].where((df['msl'] <= msl_upper) & (df['msl'] >= msl_lower), np.nan)

    # Drop rows with NaN values after global filtering
    df = df.dropna(subset=['latitude', 'longitude', 'msl'])

    return df

def v2_hybrid_outlier_filter_for_latlon_with_msl(df, rolling_window=16, rolling_factor=2, global_factor=3, global_window_factor=9):
    df = df.copy()
    
    # Rolling window-based filtering
    dfmean = df[['latitude', 'longitude', 'msl']].rolling(window=rolling_window, min_periods=1).mean()
    dfsd = df[['latitude', 'longitude', 'msl']].rolling(window=rolling_window, min_periods=1).std()

    dfulimits = dfmean + (rolling_factor * dfsd)
    dfllimits = dfmean - (rolling_factor * dfsd)

    df['latitude'] = df['latitude'].where((df['latitude'] <= dfulimits['latitude']) & (df['latitude'] >= dfllimits['latitude']), np.nan)
    df['longitude'] = df['longitude'].where((df['longitude'] <= dfulimits['longitude']) & (df['longitude'] >= dfllimits['longitude']), np.nan)
    df['msl'] = df['msl'].where((df['msl'] <= dfulimits['msl']) & (df['msl'] >= dfllimits['msl']), np.nan)

    # Drop rows with NaN values after rolling window filtering
    df = df.dropna(subset=['latitude', 'longitude', 'msl'])
    
    # Global limit-based filtering with a window size 9 times the rolling window size
    global_window = rolling_window * global_window_factor

    dfglobalmean = df[['latitude', 'longitude', 'msl']].rolling(window=global_window, min_periods=1).mean()
    dfglobalsd = df[['latitude', 'longitude', 'msl']].rolling(window=global_window, min_periods=1).std()

    dfulimits_global = dfglobalmean + (global_factor * dfglobalsd)
    dfllimits_global = dfglobalmean - (global_factor * dfglobalsd)

    df['latitude'] = df['latitude'].where((df['latitude'] <= dfulimits_global['latitude']) & (df['latitude'] >= dfllimits_global['latitude']), np.nan)
    df['longitude'] = df['longitude'].where((df['longitude'] <= dfulimits_global['longitude']) & (df['longitude'] >= dfllimits_global['longitude']), np.nan)
    df['msl'] = df['msl'].where((df['msl'] <= dfulimits_global['msl']) & (df['msl'] >= dfllimits_global['msl']), np.nan)

    # Drop rows with NaN values after global filtering
    df = df.dropna(subset=['latitude', 'longitude', 'msl'])

    return df


def resample_df(df):
    df['ts'] = pd.to_datetime(df['ts'], unit='s')
    df = df.set_index('ts').resample('10min').first().reset_index()
    return df

# Connect to the database
dyna_db = mysql.connector.connect(
            host="192.168.150.112",
            database="analysis_db",
            user="pysys_local",
            password="NaCAhztBgYZ3HwTkvHwwGVtJn5sVMFgg",
            )

# Query data
# query = "SELECT * FROM analysis_db.gnss_nagua where ts between '2024-05-10' and '2024-06-10' order by ts"
# query = "SELECT * FROM analysis_db.gnss_nagua where ts > '2024-03-22' order by ts"
#query = "SELECT * FROM analysis_db.gnss_nagua where ts > '2024-04-22' order by ts"
# query = "SELECT * FROM analysis_db.gnss_nagua where ts > '2024-05-01' order by ts"
# query = "SELECT * FROM analysis_db.gnss_sinua where ts > '2024-03-18' order by ts"
# query = "SELECT ts, latitude, longitude, msl FROM old_gnss_sinsa UNION ALL SELECT ts, latitude, longitude, msl FROM gnss_sinua order by ts"
# query = """SELECT ts, latitude, longitude, msl 
#             FROM old_gnss_sinsa 
#             WHERE ts BETWEEN '2022-07-27' AND '2023-06-10' 
#             UNION ALL 
#             SELECT ts, latitude, longitude, msl 
#             FROM gnss_sinua 
#             WHERE ts > '2024-03-17' 
#             ORDER BY ts"""
# query = "SELECT * FROM analysis_db.old_gnss_sinsa where ts between '2022-07-27' and '2023-06-10' order by ts"
# query = "SELECT * FROM analysis_db.gnss_sinua where ts > '2024-03-18' order by ts"

query = "SELECT * FROM analysis_db.gnss_tesua where ts > '2024-08-09 16:00:00' order by ts"
df = pd.read_sql(query, dyna_db)

#resample + ffillna
df = resample_df(df)
df = df.fillna(method='ffill')

# # Fixed coordinates - NAG
# fixed_lat = 16.6267267
# fixed_lon = 120.417167

# # Fixed coordinates - SIN
# fixed_lat = 16.723467
# fixed_lon = 120.7812924

# # Fixed coordinates - TES
# fixed_lat = 14.6519327
# fixed_lon = 121.0584508

# Fixed coordinates - assumed rover position
fixed_lat = 14.651944
fixed_lon = 121.058402
fixed_lat, fixed_lon = convert_to_utm(fixed_lat, fixed_lon)

# Calculate distances
df['distance_cm'] = df.apply(lambda row: euclidean_distance(row['latitude'], row['longitude'], fixed_lat, fixed_lon), axis=1)
print('df len = ', len(df))

# Unfiltered plot
# fig, axs = plt.subplots(4, 1, sharex=True, figsize=(12, 12))
# fig.suptitle('NAGUA (hybrid v2 outlier plot)')

# Unfiltered plot (optional)
# axs[0].plot(df['ts'], df['latitude'], 'b-', alpha=0.3)
# axs[1].plot(df['ts'], df['longitude'], 'g-', alpha=0.3)
# axs[2].plot(df['ts'], df['msl'], 'r-', alpha=0.3)
# axs[3].plot(df['ts'], df['distance'], 'k-', alpha=0.3)

# Apply filters
df_filtered_sanity = prepare_and_apply_sanity_filters(df, hacc=0.0141, vacc=0.0141)
print('df_filtered sanity len = ', len(df_filtered_sanity))

df_filtered_outlier = outlier_filter_for_latlon_with_msl(df_filtered_sanity)
print('df_filtered outlier len = ', len(df_filtered_outlier))

# df_filtered_hybrid = hybrid_outlier_filter_for_latlon_with_msl(df_filtered_sanity)
# print('df_filtered hybrid len = ', len(df_filtered_hybrid))

# df_filtered_hybrid_v2 = v2_hybrid_outlier_filter_for_latlon_with_msl(df_filtered_sanity)
# print('df_filtered hybrid len = ', len(df_filtered_hybrid_v2))

# df_filtered = df
# Filtered plot with labels and legends
# axs[0].plot(df_filtered['ts'], df_filtered['latitude'], 'b-', label='Filtered Latitude')
# axs[0].set_ylabel('Latitude')
# axs[0].legend()
# axs[0].set_title('Latitude vs Timestamp')

# axs[1].plot(df_filtered['ts'], df_filtered['longitude'], 'g-', label='Filtered Longitude')
# axs[1].set_ylabel('Longitude')
# axs[1].legend()
# axs[1].set_title('Longitude vs Timestamp')

# axs[2].plot(df_filtered['ts'], df_filtered['msl'], 'r-', label='Filtered MSL')
# axs[2].set_ylabel('MSL')
# axs[2].legend()
# axs[2].set_title('MSL vs Timestamp')

# axs[3].plot(df_filtered['ts'], df_filtered['distance'], 'k-', label='Filtered Distance')
# axs[3].set_ylabel('Distance (cm)')
# axs[3].set_xlabel('Timestamp')
# axs[3].legend()
# axs[3].set_title('Distance vs Timestamp')

# # Show legends
# for ax in axs:
#     ax.legend()



# df_filtered = df_filtered_outlier #change here if outlier or hybrid
# df_filtered = df_filtered_hybrid
# df_filtered = df_filtered_hybrid_v2

df_filtered = df

# # Scalar formatter for y-axis
# formatter = ScalarFormatter(useOffset=False, useMathText=False)
# formatter.set_scientific(False)

# # Filtered plot with labels and legends
# axs[0].plot(df_filtered['ts'], df_filtered['latitude'], 'b-', label='Filtered Latitude')
# axs[0].set_ylabel('Latitude')
# axs[0].yaxis.set_major_formatter(formatter)
# axs[0].legend()
# axs[0].set_title('Latitude vs Timestamp')

# axs[1].plot(df_filtered['ts'], df_filtered['longitude'], 'g-', label='Filtered Longitude')
# axs[1].set_ylabel('Longitude')
# axs[1].yaxis.set_major_formatter(formatter)
# axs[1].legend()
# axs[1].set_title('Longitude vs Timestamp')

# axs[2].plot(df_filtered['ts'], df_filtered['msl'], 'r-', label='Filtered MSL')
# axs[2].set_ylabel('MSL')
# axs[2].yaxis.set_major_formatter(formatter)
# axs[2].legend()
# axs[2].set_title('MSL vs Timestamp')

# axs[3].plot(df_filtered['ts'], df_filtered['distance'], 'k-', label='Filtered Distance')
# axs[3].set_ylabel('Distance (cm)')
# axs[3].set_xlabel('Timestamp')
# axs[3].yaxis.set_major_formatter(formatter)
# axs[3].legend()
# axs[3].set_title('Distance vs Timestamp')

# # Show legends
# for ax in axs:
#     ax.legend()



# plt.show()

# # Close the connection
# dyna_db.close()



#######################
# # Calculate distances
# df['distance'] = df.apply(lambda row: euclidean_distance(row['latitude'], row['longitude'], fixed_lat, fixed_lon), axis=1)
# print('df len = ', len(df))

# # # Apply filters
# df_filtered = prepare_and_apply_sanity_filters(df, hacc=0.0141, vacc=0.0121)
# print('df_filtered 1 len = ', len(df_filtered))
# df_filtered = outlier_filter_for_latlon_with_msl(df)
# print('df_filtered 2 len = ', len(df_filtered))

# df_filtered=df

# # Convert timestamps to strings for categorical plotting
# # df_filtered['ts'] = df_filtered['ts'].astype(str)

# # Ensure 'ts' column is in datetime format
# df_filtered['ts'] = pd.to_datetime(df_filtered['ts'])

# # Plotting
# fig, axs = plt.subplots(4, 1, sharex=True, figsize=(12, 12))
# fig.suptitle('Data Analysis')

# # Create an index for the available data points
# index = range(len(df_filtered))

# # Filtered plot with labels and legends
# axs[0].plot(index, df_filtered['latitude'], 'b-', label='Filtered Latitude')
# axs[0].set_ylabel('Latitude')
# axs[0].legend()
# axs[0].set_title('Latitude vs Timestamp')

# axs[1].plot(index, df_filtered['longitude'], 'g-', label='Filtered Longitude')
# axs[1].set_ylabel('Longitude')
# axs[1].legend()
# axs[1].set_title('Longitude vs Timestamp')

# axs[2].plot(index, df_filtered['msl'], 'r-', label='Filtered MSL')
# axs[2].set_ylabel('MSL')
# axs[2].legend()
# axs[2].set_title('MSL vs Timestamp')

# axs[3].plot(index, df_filtered['distance'], 'k-', label='Filtered Distance')
# axs[3].set_ylabel('Distance (cm)')
# axs[3].set_xlabel('Timestamp')
# axs[3].legend()
# axs[3].set_title('Distance vs Timestamp')

# # # Set x-tick labels for the fourth subplot
# # axs[3].set_xticks(index[::len(index)//10])  # Limit the number of ticks
# # axs[3].set_xticklabels(df_filtered['ts'].dt.strftime('%Y-%m-%d %H:%M').iloc[::len(index)//10], rotation=45)

# # # Suppress x-ticks for the first three subplots
# # for ax in axs[:-1]:
# #     ax.set_xticklabels([])

# # plt.tight_layout(rect=[0, 0.03, 1, 0.95])
# plt.show()

# Close the connection
dyna_db.close()