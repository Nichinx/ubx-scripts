# -*- coding: utf-8 -*-
"""
Created on Thu Jun 13 15:33:10 2024

@author: nichm

Script 2: Median and Kalman Filter
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy.ndimage import median_filter

# Load the dataset
df = pd.read_csv('gnss_nagua_0510_0610.csv')
df['ts'] = pd.to_datetime(df['ts'])

# Apply sanity filters
def prepare_and_apply_sanity_filters(df, hacc, vacc):
    df['msl'] = np.round(df['msl'], 2)
    df = df[(df['fix_type'] == 2) & (df['sat_num'] > 28)]

    if df.empty:
        return df

    df = df[(df['hacc'] == hacc) & (df['vacc'] <= vacc)]
    df = df.reset_index(drop=True).sort_values(by='ts', ascending=True, ignore_index=True)

    return df

# Apply median filter
def apply_median_filter(df, window_size=5):
    df = df.copy()
    df['latitude'] = median_filter(df['latitude'], size=window_size)
    df['longitude'] = median_filter(df['longitude'], size=window_size)
    df['msl'] = median_filter(df['msl'], size=window_size)
    return df

# Define a simple Kalman filter function
def simple_kalman_filter(data, process_variance, measurement_variance):
    estimate = np.zeros(len(data))
    estimate_error = np.zeros(len(data))
    kalman_gain = np.zeros(len(data))

    estimate[0] = data[0]
    estimate_error[0] = 1.0

    for k in range(1, len(data)):
        kalman_gain[k] = estimate_error[k - 1] / (estimate_error[k - 1] + measurement_variance)
        estimate[k] = estimate[k - 1] + kalman_gain[k] * (data[k] - estimate[k - 1])
        estimate_error[k] = (1 - kalman_gain[k]) * estimate_error[k - 1] + abs(estimate[k] - estimate[k - 1]) * process_variance

    return estimate

# Apply Kalman filter to each column
def apply_simple_kalman_filter(df, process_variance=1e-5, measurement_variance=1e-1):
    df = df.copy()
    df['latitude'] = simple_kalman_filter(df['latitude'].values, process_variance, measurement_variance)
    df['longitude'] = simple_kalman_filter(df['longitude'].values, process_variance, measurement_variance)
    df['msl'] = simple_kalman_filter(df['msl'].values, process_variance, measurement_variance)
    return df

# Combine median and Kalman filtering
def combined_filtering_simple_kalman(df, median_window_size=5, process_variance=1e-5, measurement_variance=1e-1):
    df = prepare_and_apply_sanity_filters(df, hacc=0.0141, vacc=0.01205)
    df_median_filtered = apply_median_filter(df, window_size=median_window_size)
    df_filtered = apply_simple_kalman_filter(df_median_filtered, process_variance, measurement_variance)
    return df_filtered

# Apply the combined filtering to your dataset
filtered_df_combined_simple_kalman = combined_filtering_simple_kalman(df, median_window_size=5)

# Calculate and print the percentage of data retained
raw_data_count = len(df)
filtered_data_count_combined_kalman = len(filtered_df_combined_simple_kalman)
percentage_retained_combined_kalman = (filtered_data_count_combined_kalman / raw_data_count) * 100
print(f"Percentage of data retained after Median + Simple Kalman filtering: {percentage_retained_combined_kalman:.2f}%")

   
# Plot the original and filtered data for comparison
def plot_comparison(before_df, after_df, plot_raw=True):
    fig, axs = plt.subplots(3, 1, figsize=(14, 15))
    
    if plot_raw:
        axs[0].plot(before_df['ts'], before_df['latitude'], label='Original', alpha=0.5)
    axs[0].plot(after_df['ts'], after_df['latitude'], label='Median + Simple Kalman', alpha=0.7)
    axs[0].set_xlabel('Timestamp')
    axs[0].set_ylabel('Latitude')
    axs[0].legend()
    
    if plot_raw:
        axs[1].plot(before_df['ts'], before_df['longitude'], label='Original', alpha=0.5)
    axs[1].plot(after_df['ts'], after_df['longitude'], label='Median + Simple Kalman', alpha=0.7)
    axs[1].set_xlabel('Timestamp')
    axs[1].set_ylabel('Longitude')
    axs[1].legend()
    
    if plot_raw:
        axs[2].plot(before_df['ts'], before_df['msl'], label='Original', alpha=0.5)
    axs[2].plot(after_df['ts'], after_df['msl'], label='Median + Simple Kalman', alpha=0.7)
    axs[2].set_xlabel('Timestamp')
    axs[2].set_ylabel('MSL')
    axs[2].legend()
    
    plt.tight_layout()
    plt.show()

# Plot comparison (set plot_raw to False to hide the raw data)
plot_comparison(df, filtered_df_combined_simple_kalman, plot_raw=False)
