# -*- coding: utf-8 -*-
"""
Created on Thu Jun 13 15:26:00 2024

@author: nichm

Script 1: Median and Outlier Filters
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

# Define the outlier filter function with adjustable window size
def outlier_filter_for_latlon_with_msl(df, std_multiplier=3, window_size=20):
    df = df.copy()
    dfmean = df[['latitude', 'longitude', 'msl']].rolling(window=window_size, min_periods=1).mean()
    dfsd = df[['latitude', 'longitude', 'msl']].rolling(window=window_size, min_periods=1).std()

    dfulimits = dfmean + (std_multiplier * dfsd)
    dfllimits = dfmean - (std_multiplier * dfsd)

    df['latitude'] = df['latitude'].where((df['latitude'] <= dfulimits['latitude']) & (df['latitude'] >= dfllimits['latitude']), np.nan)
    df['longitude'] = df['longitude'].where((df['longitude'] <= dfulimits['longitude']) & (df['longitude'] >= dfllimits['longitude']), np.nan)
    df['msl'] = df['msl'].where((df['msl'] <= dfulimits['msl']) & (df['msl'] >= dfllimits['msl']), np.nan)

    df = df.dropna(subset=['latitude', 'longitude', 'msl'])

    return df

# Combine median and outlier filtering
def combined_filtering(df, median_window_size=5, std_window_size=20, std_multiplier=3):
    df = prepare_and_apply_sanity_filters(df, hacc=0.0141, vacc=0.01205)
    df_median_filtered = apply_median_filter(df, window_size=median_window_size)
    df_filtered = outlier_filter_for_latlon_with_msl(df_median_filtered, std_multiplier=std_multiplier, window_size=std_window_size)
    return df_filtered

# Apply the combined filtering to your dataset
filtered_df_median_std = combined_filtering(df, median_window_size=5, std_window_size=20, std_multiplier=3)

# Calculate and print the percentage of data retained
raw_data_count = len(df)
filtered_data_count_median_std = len(filtered_df_median_std)
percentage_retained_median_std = (filtered_data_count_median_std / raw_data_count) * 100
print(f"Percentage of data retained after Median + 3 std filtering: {percentage_retained_median_std:.2f}%")
print("Raw data count: ", len(df))
print("Filtered data count: ", len(filtered_df_median_std))

# Plot the original and filtered data for comparison
def plot_comparison(before_df, after_df, plot_raw=True):
    fig, axs = plt.subplots(3, 1, figsize=(14, 15))
    
    if plot_raw:
        axs[0].plot(before_df['ts'], before_df['latitude'], label='Original', alpha=0.5)
    axs[0].plot(after_df['ts'], after_df['latitude'], label='Median + 3 std', alpha=0.7)
    axs[0].set_xlabel('Timestamp')
    axs[0].set_ylabel('Latitude')
    axs[0].legend()
    
    if plot_raw:
        axs[1].plot(before_df['ts'], before_df['longitude'], label='Original', alpha=0.5)
    axs[1].plot(after_df['ts'], after_df['longitude'], label='Median + 3 std', alpha=0.7)
    axs[1].set_xlabel('Timestamp')
    axs[1].set_ylabel('Longitude')
    axs[1].legend()
    
    if plot_raw:
        axs[2].plot(before_df['ts'], before_df['msl'], label='Original', alpha=0.5)
    axs[2].plot(after_df['ts'], after_df['msl'], label='Median + 3 std', alpha=0.7)
    axs[2].set_xlabel('Timestamp')
    axs[2].set_ylabel('MSL')
    axs[2].legend()
    
    plt.tight_layout()
    plt.show()

# Plot comparison (set plot_raw to False to hide the raw data)
plot_comparison(df, filtered_df_median_std, plot_raw=False)
