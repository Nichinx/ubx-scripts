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
    df['msl'] = np.round(df['msl'], 2)
    df = df[(df['fix_type'] == 2) & (df['sat_num'] > 28)]

    if df.empty:
        return df

    df = df[(df['hacc'] == hacc) & (df['vacc'] <= vacc)]
    df = df.reset_index(drop=True).sort_values(by='ts', ascending=True, ignore_index=True)

    return df

def outlier_filter_for_latlon_with_msl(df):
    df = df.copy()
    dfmean = df[['latitude', 'longitude', 'msl']].rolling(window=8, min_periods=1).mean()
    dfsd = df[['latitude', 'longitude', 'msl']].rolling(window=8, min_periods=1).std()

    dfulimits = dfmean + (3 * dfsd)  # 1 std
    dfllimits = dfmean - (3 * dfsd)

    df['latitude'] = df['latitude'].where((df['latitude'] <= dfulimits['latitude']) & (df['latitude'] >= dfllimits['latitude']), np.nan)
    df['longitude'] = df['longitude'].where((df['longitude'] <= dfulimits['longitude']) & (df['longitude'] >= dfllimits['longitude']), np.nan)
    df['msl'] = df['msl'].where((df['msl'] <= dfulimits['msl']) & (df['msl'] >= dfllimits['msl']), np.nan)

    df = df.dropna(subset=['latitude', 'longitude', 'msl'])

    return df

# Connect to the database
dyna_db = mysql.connector.connect(
            host="192.168.150.112",
            database="analysis_db",
            user="pysys_local",
            password="NaCAhztBgYZ3HwTkvHwwGVtJn5sVMFgg",
            )

# Query data
query = "SELECT * FROM analysis_db.gnss_nagua where ts between '2024-05-10' and '2024-06-10' order by ts"
df = pd.read_sql(query, dyna_db)

# Fixed coordinates
fixed_lat = 16.6267267
fixed_lon = 120.417167

# Calculate distances
df['distance'] = df.apply(lambda row: euclidean_distance(row['latitude'], row['longitude'], fixed_lat, fixed_lon), axis=1)
print('df len = ', len(df))

# Unfiltered plot
fig, axs = plt.subplots(4, 1, sharex=True, figsize=(12, 12))
fig.suptitle('Data Analysis')

# Unfiltered plot (optional)
axs[0].plot(df['ts'], df['latitude'], 'b-', alpha=0.3)
axs[1].plot(df['ts'], df['longitude'], 'g-', alpha=0.3)
axs[2].plot(df['ts'], df['msl'], 'r-', alpha=0.3)
axs[3].plot(df['ts'], df['distance'], 'k-', alpha=0.3)

# # Apply filters
# df_filtered = prepare_and_apply_sanity_filters(df, hacc=0.0141, vacc=0.0121)
# print('df_filtered 1 len = ', len(df_filtered))
# df_filtered = outlier_filter_for_latlon_with_msl(df_filtered)
# print('df_filtered 2 len = ', len(df_filtered))

# # Filtered plot with labels and legends
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

# Show legends
for ax in axs:
    ax.legend()

plt.show()


# Close the connection
dyna_db.close()
