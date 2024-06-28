# -*- coding: utf-8 -*-
"""
Created on Sat Jun 29 03:13:02 2024

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



# Connect to the database
dyna_db = mysql.connector.connect(
            host="192.168.150.112",
            database="analysis_db",
            user="pysys_local",
            password="NaCAhztBgYZ3HwTkvHwwGVtJn5sVMFgg",
            )


query_old = "SELECT * FROM analysis_db.old_gnss_sinsa where ts between '2022-07-27' and '2023-06-10' order by ts"
query_new = "SELECT * FROM analysis_db.gnss_sinua where ts > '2024-03-18' order by ts"
df_old = pd.read_sql(query_old, dyna_db)
df_new = pd.read_sql(query_new, dyna_db)

# Standardize column names
df_old.rename(columns={'accuracy': 'hacc'}, inplace=True)

# Add missing columns with null values in df_old
df_old['vacc'] = None
df_old['sat_num'] = None

# Merge DataFrames (append)
df_merged = pd.concat([df_old, df_new], ignore_index=True)

# Sort by 'ts' column
df_merged.sort_values(by='ts', inplace=True)

# Rearrange columns if needed
df_merged = df_merged[['ts', 'fix_type', 'latitude', 'longitude', 'hacc', 'vacc', 'msl', 'sat_num', 'temp', 'volt']]

# Optionally, reset index
df_merged.reset_index(drop=True, inplace=True)
df = df_merged

# Fixed coordinates - SIN
fixed_lat = 16.723467
fixed_lon = 120.7812924


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


# Calculate distances
df['distance'] = df.apply(lambda row: euclidean_distance(row['latitude'], row['longitude'], fixed_lat, fixed_lon), axis=1)
print('df = ', len(df))


def prepare_and_apply_sanity_filters(df, hacc, vacc):
    # Fill None values in vacc and sat_num
    df['vacc'].fillna(np.inf, inplace=True)
    df['sat_num'].fillna(0, inplace=True)
    
    df['msl'] = np.round(df['msl'], 2)
    df = df[(df['fix_type'] == 2)] #& (df['sat_num'] > 28)]

    if df.empty:
        return df

    df = df[(df['hacc'] == hacc)] #& (df['vacc'] <= vacc)]
    df = df.reset_index(drop=True).sort_values(by='ts', ascending=True, ignore_index=True)

    return df

def outlier_filter_for_latlon_with_msl(df):
    df = df.copy()
    dfmean = df[['latitude', 'longitude', 'msl']].rolling(window=20, min_periods=1).mean()
    dfsd = df[['latitude', 'longitude', 'msl']].rolling(window=20, min_periods=1).std()

    dfulimits = dfmean + (2 * dfsd)  # 2 std
    dfllimits = dfmean - (2 * dfsd)

    df['latitude'] = df['latitude'].where((df['latitude'] <= dfulimits['latitude']) & (df['latitude'] >= dfllimits['latitude']), np.nan)
    df['longitude'] = df['longitude'].where((df['longitude'] <= dfulimits['longitude']) & (df['longitude'] >= dfllimits['longitude']), np.nan)
    df['msl'] = df['msl'].where((df['msl'] <= dfulimits['msl']) & (df['msl'] >= dfllimits['msl']), np.nan)

    df = df.dropna(subset=['latitude', 'longitude', 'msl'])

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


# Apply filters
df_filtered_sanity = prepare_and_apply_sanity_filters(df, hacc=0.0141, vacc=0.0121)
print('df_filtered sanity len = ', len(df_filtered_sanity))

df_filtered_outlier = outlier_filter_for_latlon_with_msl(df_filtered_sanity)
print('df_filtered outlier len = ', len(df_filtered_outlier))

df_filtered_hybrid = hybrid_outlier_filter_for_latlon_with_msl(df_filtered_sanity)
print('df_filtered hybrid len = ', len(df_filtered_hybrid))


df_filtered = df_filtered_outlier #change if outlier or hybrid


# Unfiltered plot
fig, axs = plt.subplots(4, 1, sharex=True, figsize=(12, 12))
fig.suptitle('2022 to 2024 SINUA (outlier plot)')

# Unfiltered plot (optional)
# axs[0].plot(df['ts'], df['latitude'], 'b-', alpha=0.3)
# axs[1].plot(df['ts'], df['longitude'], 'g-', alpha=0.3)
# axs[2].plot(df['ts'], df['msl'], 'r-', alpha=0.3)
# axs[3].plot(df['ts'], df['distance'], 'k-', alpha=0.3)


# Scalar formatter for y-axis
formatter = ScalarFormatter(useOffset=False, useMathText=False)
formatter.set_scientific(False)

# Filtered plot with labels and legends
axs[0].plot(df_filtered['ts'], df_filtered['latitude'], 'b-', label='Filtered Latitude')
axs[0].set_ylabel('Latitude')
axs[0].yaxis.set_major_formatter(formatter)
axs[0].legend()
axs[0].set_title('Latitude vs Timestamp')

axs[1].plot(df_filtered['ts'], df_filtered['longitude'], 'g-', label='Filtered Longitude')
axs[1].set_ylabel('Longitude')
axs[1].yaxis.set_major_formatter(formatter)
axs[1].legend()
axs[1].set_title('Longitude vs Timestamp')

axs[2].plot(df_filtered['ts'], df_filtered['msl'], 'r-', label='Filtered MSL')
axs[2].set_ylabel('MSL')
axs[2].yaxis.set_major_formatter(formatter)
axs[2].legend()
axs[2].set_title('MSL vs Timestamp')

axs[3].plot(df_filtered['ts'], df_filtered['distance'], 'k-', label='Filtered Distance')
axs[3].set_ylabel('Distance (cm)')
axs[3].set_xlabel('Timestamp')
axs[3].yaxis.set_major_formatter(formatter)
axs[3].legend()
axs[3].set_title('Distance vs Timestamp')

# Show legends
for ax in axs:
    ax.legend()

plt.show()


# Close the connection
dyna_db.close()
