# -*- coding: utf-8 -*-
"""
Created on Wed Jun  5 16:11:44 2024

@author: nichm
"""

import numpy as np
import math
import matplotlib.pyplot as plt
import pandas as pd

# Load the CSV file
file_path = 'gnss_nagua_0510_0610.csv'
data = pd.read_csv(file_path)

# Display the first few rows of the dataframe
# data.head(), data.columns


# Function to compute the Euclidean distance
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

# Fixed coordinates
fixed_lat = 16.6267267
fixed_lon = 120.417167

# Compute distances
data['distance'] = data.apply(lambda row: euclidean_distance(row['latitude'], row['longitude'], fixed_lat, fixed_lon), axis=1)

# Convert timestamp column to datetime
data['ts'] = pd.to_datetime(data['ts'])

# Define the sanity filter function
def prepare_and_apply_sanity_filters(df, horizontal_accuracy, vertical_accuracy):
    df['msl'] = np.round(df['msl'], 2)
    df = df[(df['fix_type'] == 2) & (df['sat_num'] > 28)]

    if df.empty:
        return df

    df = df[(df['hacc'] == horizontal_accuracy) & (df['vacc'] <= vertical_accuracy)]
    df = df.reset_index(drop=True).sort_values(by='ts', ascending=True, ignore_index=True)

    return df


# Define the outlier filter function
def outlier_filter_for_latlon_with_msl(df):
    df = df.copy()
    dfmean = df[['latitude', 'longitude', 'msl']].rolling(window=8, min_periods=1).mean()
    dfsd = df[['latitude', 'longitude', 'msl']].rolling(window=8, min_periods=1).std()

    dfulimits = dfmean + (1 * dfsd)  # 1 std
    dfllimits = dfmean - (1 * dfsd)

    df['latitude'] = df['latitude'].where((df['latitude'] <= dfulimits['latitude']) & (df['latitude'] >= dfllimits['latitude']), np.nan)
    df['longitude'] = df['longitude'].where((df['longitude'] <= dfulimits['longitude']) & (df['longitude'] >= dfllimits['longitude']), np.nan)
    df['msl'] = df['msl'].where((df['msl'] <= dfulimits['msl']) & (df['msl'] >= dfllimits['msl']), np.nan)

    df = df.dropna(subset=['latitude', 'longitude', 'msl'])

    return df


############# unfiltered

# # Plotting
# fig, ax1 = plt.subplots()

# # Plot distance vs ts
# ax1.set_xlabel('Timestamp')
# ax1.set_ylabel('Distance (cm)', color='tab:blue')
# ax1.plot(data['ts'], data['distance'], color='tab:blue', label='Distance')
# ax1.tick_params(axis='y', labelcolor='tab:blue')

# # Create a second y-axis for temperature
# ax2 = ax1.twinx()
# ax2.set_ylabel('Temperature (°C)', color='tab:red')
# ax2.plot(data['ts'], data['temp'], color='tab:red', label='Temperature')
# ax2.tick_params(axis='y', labelcolor='tab:red')

# # Title and legend
# fig.suptitle('Distance and Temperature vs. Timestamp')
# fig.tight_layout()
# fig.legend(loc='upper left', bbox_to_anchor=(0.1,0.9))

# # Show plot
# plt.show()


########################## filtered

# # Apply the outlier filter
# filtered_data = outlier_filter_for_latlon_with_msl(data)

# # Compute distances after filtering
# filtered_data['distance'] = filtered_data.apply(lambda row: euclidean_distance(row['latitude'], row['longitude'], fixed_lat, fixed_lon), axis=1)

# # Plotting with filtered data
# fig, ax1 = plt.subplots()

# # Plot distance vs ts
# ax1.set_xlabel('Timestamp')
# ax1.set_ylabel('Distance (cm)', color='tab:blue')
# ax1.plot(filtered_data['ts'], filtered_data['distance'], color='tab:blue', label='Distance')
# ax1.tick_params(axis='y', labelcolor='tab:blue')

# # Create a second y-axis for temperature
# ax2 = ax1.twinx()
# ax2.set_ylabel('Temperature (°C)', color='tab:red')
# ax2.plot(filtered_data['ts'], filtered_data['temp'], color='tab:red', label='Temperature')
# ax2.tick_params(axis='y', labelcolor='tab:red')

# # Title and legend
# fig.suptitle('Distance and Temperature vs. Timestamp (Filtered)')
# fig.tight_layout()
# fig.legend(loc='upper left', bbox_to_anchor=(0.1,0.9))

# # Show plot
# plt.show()



################## both

# # Compute distances for unfiltered data
# data['distance'] = data.apply(lambda row: euclidean_distance(row['latitude'], row['longitude'], fixed_lat, fixed_lon), axis=1)

# # Plotting both filtered and unfiltered data
# fig, ax1 = plt.subplots()

# # Plot unfiltered distance vs ts
# ax1.set_xlabel('Timestamp')
# ax1.set_ylabel('Distance (cm)')
# ax1.plot(data['ts'], data['distance'], color='tab:green', alpha=0.4, label='Distance (Unfiltered)')
# ax1.plot(filtered_data['ts'], filtered_data['distance'], color='tab:blue', label='Distance (Filtered)')

# # Create a second y-axis for temperature
# ax2 = ax1.twinx()
# ax2.set_ylabel('Temperature (°C)')
# ax2.plot(data['ts'], data['temp'], color='tab:orange', alpha=0.4, label='Temperature (Unfiltered)')
# ax2.plot(filtered_data['ts'], filtered_data['temp'], color='tab:red', label='Temperature (Filtered)')

# # Title and legend
# fig.suptitle('Distance and Temperature vs. Timestamp (Filtered and Unfiltered)')
# fig.tight_layout()
# fig.legend(loc='upper left', bbox_to_anchor=(0.1,0.9))

# # Show plot
# plt.show()


# Apply sanity filters
horizontal_accuracy = 0.0141  # example value, adjust as needed
vertical_accuracy = 0.0122  # example value, adjust as needed
filtered_sanity_data = prepare_and_apply_sanity_filters(data, horizontal_accuracy, vertical_accuracy)

# Apply the outlier filter
filtered_data = outlier_filter_for_latlon_with_msl(filtered_sanity_data)
filtered_data['ts'] = pd.to_datetime(filtered_data['ts'])

# Compute distances for unfiltered data
data['distance'] = data.apply(lambda row: euclidean_distance(row['latitude'], row['longitude'], fixed_lat, fixed_lon), axis=1)

# Compute distances for filtered data
filtered_data['distance'] = filtered_data.apply(lambda row: euclidean_distance(row['latitude'], row['longitude'], fixed_lat, fixed_lon), axis=1)

# Save the filtered dataframe to a CSV file
filtered_data.to_csv('filtered_data_with_distance.csv', index=False)

# # Plotting both filtered and unfiltered data
# fig, ax1 = plt.subplots()

# # Plot unfiltered distance vs ts
# ax1.set_xlabel('Timestamp')
# ax1.set_ylabel('Distance (cm)')
# # ax1.plot(data['ts'], data['distance'], color='tab:blue', alpha=0.3, label='Distance (Unfiltered)')
# ax1.plot(filtered_data['ts'], filtered_data['distance'], color='tab:blue', label='Distance (Filtered)')

# # Create a second y-axis for temperature
# ax2 = ax1.twinx()
# ax2.set_ylabel('Temperature (°C)')
# # ax2.plot(data['ts'], data['temp'], color='tab:red', alpha=0.3, label='Temperature (Unfiltered)')
# ax2.plot(filtered_data['ts'], filtered_data['temp'], color='tab:red', label='Temperature (Filtered)')

# # Title and legend
# fig.suptitle('Distance and Temperature vs. Timestamp (Filtered and Unfiltered)')
# fig.tight_layout()
# fig.legend(loc='upper left', bbox_to_anchor=(0.1,0.9))

# # Show plot
# plt.show()


# # Plotting msl and temp vs ts
# fig, ax1 = plt.subplots()

# # Plot msl vs ts
# ax1.set_xlabel('Timestamp')
# ax1.set_ylabel('MSL (meters)', color='tab:blue')
# ax1.plot(filtered_data['ts'], filtered_data['msl'], color='tab:blue', label='MSL')
# ax1.tick_params(axis='y', labelcolor='tab:blue')

# # Create a second y-axis for temperature
# ax2 = ax1.twinx()
# ax2.set_ylabel('Temperature (°C)', color='tab:red')
# ax2.plot(filtered_data['ts'], filtered_data['temp'], color='tab:red', label='Temperature')
# ax2.tick_params(axis='y', labelcolor='tab:red')

# # Title and legend
# fig.suptitle('MSL and Temperature vs. Timestamp')
# fig.tight_layout()
# fig.legend(loc='upper left', bbox_to_anchor=(0.1,0.9))