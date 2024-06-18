# -*- coding: utf-8 -*-
"""
Created on Thu Jun 13 19:55:32 2024

@author: nichm
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from pykalman import KalmanFilter

# Load the GNSS/GPS data from the provided CSV file
file_path = 'gnss_nagua_0510_0610.csv'
data = pd.read_csv(file_path)

# Define the rolling window outlier filter function
def outlier_filter_for_latlon_with_msl(df):
    df = df.copy()
    dfmean = df[['latitude', 'longitude', 'msl']].rolling(window=20, min_periods=1).mean()
    dfsd = df[['latitude', 'longitude', 'msl']].rolling(window=20, min_periods=1).std()

    dfulimits = dfmean + (2 * dfsd)  # 1 std
    dfllimits = dfmean - (2 * dfsd)

    df['latitude'] = df['latitude'].where((df['latitude'] <= dfulimits['latitude']) & (df['latitude'] >= dfllimits['latitude']), np.nan)
    df['longitude'] = df['longitude'].where((df['longitude'] <= dfulimits['longitude']) & (df['longitude'] >= dfllimits['longitude']), np.nan)
    df['msl'] = df['msl'].where((df['msl'] <= dfulimits['msl']) & (df['msl'] >= dfllimits['msl']), np.nan)

    df = df.dropna(subset=['latitude', 'longitude', 'msl'])

    return df

# Initial data size
initial_data_size = len(data)

# Step 1: Remove Fix Type Outliers (assuming fix type 2 is a higher quality)
filtered_data = data[data['fix_type'] == 2]

# Step 2: Apply the rolling window outlier filter
filtered_data = outlier_filter_for_latlon_with_msl(filtered_data)

# Step 3: Apply the moving average filter
# Define the window size for moving average
window_size = 5

# Apply moving average filter to latitude, longitude, and msl
filtered_data['latitude_smooth'] = filtered_data['latitude'].rolling(window=window_size, center=True).mean()
filtered_data['longitude_smooth'] = filtered_data['longitude'].rolling(window=window_size, center=True).mean()
filtered_data['msl_smooth'] = filtered_data['msl'].rolling(window=window_size, center=True).mean()

# Drop rows with NaN values resulted from smoothing
filtered_data.dropna(subset=['latitude_smooth', 'longitude_smooth', 'msl_smooth'], inplace=True)

# Convert the timestamp column to datetime format for better plotting
filtered_data['ts'] = pd.to_datetime(filtered_data['ts'])

# Apply Kalman Filter using pykalman
# Prepare the data for Kalman Filter
observations = filtered_data[['latitude_smooth', 'longitude_smooth', 'msl_smooth']].values

# Initialize the Kalman Filter
kf = KalmanFilter(initial_state_mean=observations[0], n_dim_obs=3)  # We have 3D observations: latitude, longitude, and msl

# Apply the Kalman Filter
state_means, _ = kf.smooth(observations)

# Add the Kalman Filter results to the dataframe
filtered_data['latitude_kalman'] = state_means[:, 0]
filtered_data['longitude_kalman'] = state_means[:, 1]
filtered_data['msl_kalman'] = state_means[:, 2]

# Calculate the percentage of data remaining
final_data_size = len(filtered_data)
percentage_remaining = (final_data_size / initial_data_size) * 100

print(f"Initial data size: {initial_data_size}")
print(f"Final data size: {final_data_size}")
print(f"Percentage of data remaining: {percentage_remaining:.2f}%")

# Plot the original, smoothed, and Kalman filtered latitude, longitude, and msl data
plt.figure(figsize=(14, 15))

# Latitude plot
plt.subplot(3, 1, 1)
plt.plot(filtered_data['ts'], filtered_data['latitude'], label='Original Latitude', alpha=0.5)
plt.plot(filtered_data['ts'], filtered_data['latitude_smooth'], label='Smoothed Latitude', alpha=0.7)
plt.plot(filtered_data['ts'], filtered_data['latitude_kalman'], label='Kalman Filtered Latitude', alpha=0.9)
plt.xlabel('Timestamp')
plt.ylabel('Latitude')
plt.legend()
plt.title('Latitude Data Filtering')
plt.gca().xaxis.set_major_locator(mdates.AutoDateLocator())
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M:%S'))
plt.xticks(rotation=45)

# Longitude plot
plt.subplot(3, 1, 2)
plt.plot(filtered_data['ts'], filtered_data['longitude'], label='Original Longitude', alpha=0.5)
plt.plot(filtered_data['ts'], filtered_data['longitude_smooth'], label='Smoothed Longitude', alpha=0.7)
plt.plot(filtered_data['ts'], filtered_data['longitude_kalman'], label='Kalman Filtered Longitude', alpha=0.9)
plt.xlabel('Timestamp')
plt.ylabel('Longitude')
plt.legend()
plt.title('Longitude Data Filtering')
plt.gca().xaxis.set_major_locator(mdates.AutoDateLocator())
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M:%S'))
plt.xticks(rotation=45)

# MSL plot
plt.subplot(3, 1, 3)
plt.plot(filtered_data['ts'], filtered_data['msl'], label='Original MSL', alpha=0.5)
plt.plot(filtered_data['ts'], filtered_data['msl_smooth'], label='Smoothed MSL', alpha=0.7)
plt.plot(filtered_data['ts'], filtered_data['msl_kalman'], label='Kalman Filtered MSL', alpha=0.9)
plt.xlabel('Timestamp')
plt.ylabel('Mean Sea Level (MSL)')
plt.legend()
plt.title('MSL Data Filtering')
plt.gca().xaxis.set_major_locator(mdates.AutoDateLocator())
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M:%S'))
plt.xticks(rotation=45)

plt.tight_layout()
plt.show()
