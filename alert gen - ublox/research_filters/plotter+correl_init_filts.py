# -*- coding: utf-8 -*-
"""
Created on Tue Jun 11 18:47:26 2024

@author: nichm
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy.stats import spearmanr, pearsonr

# Load the provided data
df = pd.read_csv('gnss_nagua_0423_0610.csv')

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

# Apply the sanity filter
horizontal_accuracy = 0.0141
vertical_accuracy = 0.01205
df_filtered = prepare_and_apply_sanity_filters(df, horizontal_accuracy, vertical_accuracy)

# Apply the outlier filter
df_filtered_outliers = outlier_filter_for_latlon_with_msl(df_filtered)

# Plotting
df['ts'] = pd.to_datetime(df['ts'])
df_filtered_outliers['ts'] = pd.to_datetime(df_filtered_outliers['ts'])

fig, axs = plt.subplots(3, 1, figsize=(12, 18), sharex=True)

# Plot 1: latitude vs ts and temp vs ts
# axs[0].plot(df['ts'], df['latitude'], 'r', alpha=0.3, label='Unfiltered Latitude')
axs[0].plot(df_filtered_outliers['ts'], df_filtered_outliers['latitude'], 'r', label='Filtered Latitude')
ax2 = axs[0].twinx()
# ax2.plot(df['ts'], df['temp'], 'b', alpha=0.3, label='Unfiltered Temp')
ax2.plot(df_filtered_outliers['ts'], df_filtered_outliers['temp'], 'b', label='Filtered Temp')
axs[0].set_title('Latitude vs Temp over Time')
axs[0].set_ylabel('Latitude')
ax2.set_ylabel('Temp')

# Plot 2: longitude vs ts and temp vs ts
# axs[1].plot(df['ts'], df['longitude'], 'g', alpha=0.3, label='Unfiltered Longitude')
axs[1].plot(df_filtered_outliers['ts'], df_filtered_outliers['longitude'], 'g', label='Filtered Longitude')
ax2 = axs[1].twinx()
# ax2.plot(df['ts'], df['temp'], 'b', alpha=0.3, label='Unfiltered Temp')
ax2.plot(df_filtered_outliers['ts'], df_filtered_outliers['temp'], 'b', label='Filtered Temp')
axs[1].set_title('Longitude vs Temp over Time')
axs[1].set_ylabel('Longitude')
ax2.set_ylabel('Temp')

# Plot 3: msl vs ts and temp vs ts
# axs[2].plot(df['ts'], df['msl'], 'y', alpha=0.3, label='Unfiltered MSL')
axs[2].plot(df_filtered_outliers['ts'], df_filtered_outliers['msl'], 'y', label='Filtered MSL')
ax2 = axs[2].twinx()
# ax2.plot(df['ts'], df['temp'], 'b', alpha=0.3, label='Unfiltered Temp')
ax2.plot(df_filtered_outliers['ts'], df_filtered_outliers['temp'], 'b', label='Filtered Temp')
axs[2].set_title('MSL vs Temp over Time')
axs[2].set_ylabel('MSL')
ax2.set_ylabel('Temp')

for ax in axs:
    ax.legend(loc='upper left')
    ax.grid(True)

plt.xlabel('Timestamp')
plt.tight_layout()
plt.show()

# Correlation analysis
correlations = {}
for col in ['latitude', 'longitude', 'msl']:
    pearson_corr = pearsonr(df[col], df['temp'])
    spearman_corr = spearmanr(df[col], df['temp'])
    correlations[col] = {
        'Pearson': pearson_corr,
        'Spearman': spearman_corr
    }

correlations
