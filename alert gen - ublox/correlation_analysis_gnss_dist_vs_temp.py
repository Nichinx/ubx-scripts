# -*- coding: utf-8 -*-
"""
Created on Fri Jun  7 17:22:26 2024

@author: nichm
"""

import pandas as pd
from scipy.stats import pearsonr, spearmanr

# Load the filtered data with distance
filtered_data = pd.read_csv('filtered_data_with_distance.csv')



# # Extract the relevant columns
# temp = filtered_data['temp']
# distance = filtered_data['distance']
# msl = filtered_data['msl']

# # Compute Pearson and Spearman correlations for distance vs temp
# pearson_corr_dist_temp, pearson_p_dist_temp = pearsonr(temp, distance)
# spearman_corr_dist_temp, spearman_p_dist_temp = spearmanr(temp, distance)

# # Compute Pearson and Spearman correlations for msl vs temp
# pearson_corr_msl_temp, pearson_p_msl_temp = pearsonr(temp, msl)
# spearman_corr_msl_temp, spearman_p_msl_temp = spearmanr(temp, msl)

# # Print results
# print("Pearson Correlation Results:")
# print(f"Distance vs Temp: Correlation = {pearson_corr_dist_temp}, p-value = {pearson_p_dist_temp}")
# print(f"MSL vs Temp: Correlation = {pearson_corr_msl_temp}, p-value = {pearson_p_msl_temp}")

# print("\nSpearman Correlation Results:")
# print(f"Distance vs Temp: Correlation = {spearman_corr_dist_temp}, p-value = {spearman_p_dist_temp}")
# print(f"MSL vs Temp: Correlation = {spearman_corr_msl_temp}, p-value = {spearman_p_msl_temp}")

# # Interpretation
# alpha = 0.05

# # Distance vs Temp Interpretation
# print("\nInterpretation of Distance vs Temp:")
# if pearson_p_dist_temp < alpha:
#     print(f"Pearson: Significant linear relationship (p < {alpha}).")
#     if pearson_corr_dist_temp > 0:
#         print("The relationship is positive (direct).")
#     elif pearson_corr_dist_temp < 0:
#         print("The relationship is negative (inverse).")
#     else:
#         print("No linear relationship.")
# else:
#     print("Pearson: No significant linear relationship (p >= 0.05).")

# if spearman_p_dist_temp < alpha:
#     print(f"Spearman: Significant monotonic relationship (p < {alpha}).")
#     if spearman_corr_dist_temp > 0:
#         print("The relationship is positive (direct).")
#     elif spearman_corr_dist_temp < 0:
#         print("The relationship is negative (inverse).")
#     else:
#         print("No monotonic relationship.")
# else:
#     print("Spearman: No significant monotonic relationship (p >= 0.05).")

# # MSL vs Temp Interpretation
# print("\nInterpretation of MSL vs Temp:")
# if pearson_p_msl_temp < alpha:
#     print(f"Pearson: Significant linear relationship (p < {alpha}).")
#     if pearson_corr_msl_temp > 0:
#         print("The relationship is positive (direct).")
#     elif pearson_corr_msl_temp < 0:
#         print("The relationship is negative (inverse).")
#     else:
#         print("No linear relationship.")
# else:
#     print("Pearson: No significant linear relationship (p >= 0.05).")

# if spearman_p_msl_temp < alpha:
#     print(f"Spearman: Significant monotonic relationship (p < {alpha}).")
#     if spearman_corr_msl_temp > 0:
#         print("The relationship is positive (direct).")
#     elif spearman_corr_msl_temp < 0:
#         print("The relationship is negative (inverse).")
#     else:
#         print("No monotonic relationship.")
# else:
#     print("Spearman: No significant monotonic relationship (p >= 0.05).")



#################PLOTTER
import matplotlib.pyplot as plt

# Convert ts column to datetime if it's not already
df = filtered_data
df['ts'] = pd.to_datetime(df['ts'])

# Create four separate plots
fig, axs = plt.subplots(4, 1, figsize=(10, 20))

# Plot 1: Latitude vs Temp
axs[0].plot(df['ts'], df['latitude'], label='Latitude', color='blue')
axs[0].set_xlabel('ts')
axs[0].set_ylabel('Latitude')
axs[0].tick_params(axis='y', labelcolor='blue')
axs[0].legend(loc='upper left')

ax2 = axs[0].twinx()
ax2.plot(df['ts'], df['temp'], label='Temp', color='red', linestyle='--')
ax2.set_ylabel('Temp')
ax2.tick_params(axis='y', labelcolor='red')
ax2.legend(loc='upper right')

# Plot 2: Longitude vs Temp
axs[1].plot(df['ts'], df['longitude'], label='Longitude', color='green')
axs[1].set_xlabel('ts')
axs[1].set_ylabel('Longitude')
axs[1].tick_params(axis='y', labelcolor='green')
axs[1].legend(loc='upper left')

ax3 = axs[1].twinx()
ax3.plot(df['ts'], df['temp'], label='Temp', color='red', linestyle='--')
ax3.set_ylabel('Temp')
ax3.tick_params(axis='y', labelcolor='red')
ax3.legend(loc='upper right')

# Plot 3: MSL vs Temp
axs[2].plot(df['ts'], df['msl'], label='MSL', color='purple')
axs[2].set_xlabel('ts')
axs[2].set_ylabel('MSL')
axs[2].tick_params(axis='y', labelcolor='purple')
axs[2].legend(loc='upper left')

ax4 = axs[2].twinx()
ax4.plot(df['ts'], df['temp'], label='Temp', color='red', linestyle='--')
ax4.set_ylabel('Temp')
ax4.tick_params(axis='y', labelcolor='red')
ax4.legend(loc='upper right')

# Plot 4: Distance vs Temp
axs[3].plot(df['ts'], df['distance'], label='Distance', color='orange')
axs[3].set_xlabel('ts')
axs[3].set_ylabel('Distance')
axs[3].tick_params(axis='y', labelcolor='orange')
axs[3].legend(loc='upper left')

ax5 = axs[3].twinx()
ax5.plot(df['ts'], df['temp'], label='Temp', color='red', linestyle='--')
ax5.set_ylabel('Temp')
ax5.tick_params(axis='y', labelcolor='red')
ax5.legend(loc='upper right')

# Adjust layout
plt.tight_layout()
plt.show()




##########CORRELATION
# Perform correlation analysis
temp = df['temp']
latitude = df['latitude']
longitude = df['longitude']
msl = df['msl']
distance = df['distance']

# Compute Pearson and Spearman correlations
pearson_corr_temp_latitude, pearson_p_temp_latitude = pearsonr(temp, latitude)
spearman_corr_temp_latitude, spearman_p_temp_latitude = spearmanr(temp, latitude)

pearson_corr_temp_longitude, pearson_p_temp_longitude = pearsonr(temp, longitude)
spearman_corr_temp_longitude, spearman_p_temp_longitude = spearmanr(temp, longitude)

pearson_corr_temp_msl, pearson_p_temp_msl = pearsonr(temp, msl)
spearman_corr_temp_msl, spearman_p_temp_msl = spearmanr(temp, msl)

pearson_corr_temp_distance, pearson_p_temp_distance = pearsonr(temp, distance)
spearman_corr_temp_distance, spearman_p_temp_distance = spearmanr(temp, distance)

# Interpretation function
def interpret_correlation_results(correlation, p_value, alpha=0.05):
    interpretation = ""
    if p_value < alpha:
        if correlation > 0:
            interpretation += "Strong positive relationship."
        elif correlation < 0:
            interpretation += "Strong negative relationship."
        else:
            interpretation += "No relationship."
        
        if abs(correlation) >= 0.7:
            interpretation += " Strong correlation. Statistically significant."
        elif abs(correlation) >= 0.3:
            interpretation += " Moderate correlation. Statistically significant."
        else:
            interpretation += " Weak correlation. Statistically significant."
    else:
        interpretation += "No statistically significant correlation."
    
    return interpretation

# Interpretation
alpha = 0.05

# Interpretation
print("\nInterpretation of Latitude vs Temp:")
print("Pearson Correlation:")
print(interpret_correlation_results(pearson_corr_temp_latitude, pearson_p_temp_latitude))
print("Spearman Correlation:")
print(interpret_correlation_results(spearman_corr_temp_latitude, spearman_p_temp_latitude))

print("\nInterpretation of Longitude vs Temp:")
print("Pearson Correlation:")
print(interpret_correlation_results(pearson_corr_temp_longitude, pearson_p_temp_longitude))
print("Spearman Correlation:")
print(interpret_correlation_results(spearman_corr_temp_longitude, spearman_p_temp_longitude))

print("\nInterpretation of MSL vs Temp:")
print("Pearson Correlation:")
print(interpret_correlation_results(pearson_corr_temp_msl, pearson_p_temp_msl))
print("Spearman Correlation:")
print(interpret_correlation_results(spearman_corr_temp_msl, spearman_p_temp_msl))

print("\nInterpretation of Distance vs Temp:")
print("Pearson Correlation:")
print(interpret_correlation_results(pearson_corr_temp_distance, pearson_p_temp_distance))
print("Spearman Correlation:")
print(interpret_correlation_results(spearman_corr_temp_distance, spearman_p_temp_distance))