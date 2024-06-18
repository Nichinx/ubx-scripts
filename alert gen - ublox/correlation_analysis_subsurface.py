# -*- coding: utf-8 -*-
"""
Created on Fri Jun  7 18:20:04 2024

@author: nichm
"""

import pandas as pd
import mysql.connector

# Connect to the database
dyna_db = mysql.connector.connect(
            host="192.168.150.112",
            database="analysis_db",
            user="pysys_local",
            password="NaCAhztBgYZ3HwTkvHwwGVtJn5sVMFgg",
            )

# Load data from the tables into DataFrames
df1_query = "SELECT * FROM tilt_imute WHERE node_id = 1 order by ts desc limit 1000"
df2_query = "SELECT * FROM temp_imute WHERE node_id = 1 order by ts desc limit 1000"


df1 = pd.read_sql(df1_query, dyna_db)
df2 = pd.read_sql(df2_query, dyna_db)

# Merge the DataFrames on the 'ts' column
merged_df = pd.merge(df1, df2, on='ts', how='inner')

# Verify the merged DataFrame
print(merged_df.head())

# Close the database connection
dyna_db.close()
dyna_db.close()


################
# import matplotlib.pyplot as plt

# # Filter data for type_num = 41
# type_41_data = merged_df[merged_df['type_num_x'] == 41]

# # Filter data for type_num = 42
# type_42_data = merged_df[merged_df['type_num_x'] == 42]

# # Create two separate plots
# fig, axs = plt.subplots(2, 3, figsize=(15, 10))  # 2 rows, 3 columns

# # Plot for type_num = 41
# for i, col in enumerate(['xval', 'yval', 'zval']):
#     axs[0, i].plot(type_41_data['ts'], type_41_data[col], label=col)
#     axs[0, i].plot(type_41_data['ts'], type_41_data['temp_val'], label='temp_val', color='red', linestyle='--')
#     axs[0, i].set_xlabel('ts')
#     axs[0, i].set_ylabel(col)
#     axs[0, i].legend()
    
#     # Add a shared x-axis for better visualization
#     if i == 0:
#         axs[0, i].set_title('type_num = 41')

# # Plot for type_num = 42
# for i, col in enumerate(['xval', 'yval', 'zval']):
#     axs[1, i].plot(type_42_data['ts'], type_42_data[col], label=col)
#     axs[1, i].plot(type_42_data['ts'], type_42_data['temp_val'], label='temp_val', color='red', linestyle='--')
#     axs[1, i].set_xlabel('ts')
#     axs[1, i].set_ylabel(col)
#     axs[1, i].legend()
    
#     # Add a shared x-axis for better visualization
#     if i == 0:
#         axs[1, i].set_title('type_num = 42')

# plt.tight_layout()
# plt.show()
####################


##PLOTTER 
import matplotlib.pyplot as plt

# Filter data for type_num = 41
type_41_data = merged_df[merged_df['type_num_x'] == 41]

# Filter data for type_num = 42
type_42_data = merged_df[merged_df['type_num_x'] == 42]

# Create two separate plots
fig, axs = plt.subplots(2, 3, figsize=(15, 10))  # 2 rows, 3 columns

# Plot for type_num = 41
for i, col in enumerate(['xval', 'yval', 'zval']):
    ax = axs[0, i]
    ax.plot(type_41_data['ts'], type_41_data[col], label=col)
    ax.set_xlabel('ts')
    ax.set_ylabel(col)
    ax.legend()
    ax2 = ax.twinx()
    ax2.plot(type_41_data['ts'], type_41_data['temp_val'], label='temp_val', color='red', linestyle='--')
    ax2.set_ylabel('temp_val')
    ax2.legend()
    if i == 0:
        ax.set_title('type_num = 41')

# Plot for type_num = 42
for i, col in enumerate(['xval', 'yval', 'zval']):
    ax = axs[1, i]
    ax.plot(type_42_data['ts'], type_42_data[col], label=col)
    ax.set_xlabel('ts')
    ax.set_ylabel(col)
    ax.legend()
    ax2 = ax.twinx()
    ax2.plot(type_42_data['ts'], type_42_data['temp_val'], label='temp_val', color='red', linestyle='--')
    ax2.set_ylabel('temp_val')
    ax2.legend()
    if i == 0:
        ax.set_title('type_num = 42')

plt.tight_layout()
plt.show()



######CORRELATION ANALYSIS
from scipy.stats import pearsonr, spearmanr

# Extract the relevant columns for type_num = 41
xval_type_41 = type_41_data['xval']
yval_type_41 = type_41_data['yval']
zval_type_41 = type_41_data['zval']
temp_type_41 = type_41_data['temp_val']

# Extract the relevant columns for type_num = 42
xval_type_42 = type_42_data['xval']
yval_type_42 = type_42_data['yval']
zval_type_42 = type_42_data['zval']
temp_type_42 = type_42_data['temp_val']

# Compute Pearson and Spearman correlations for type_num = 41
pearson_corr_xval_temp_41, pearson_p_xval_temp_41 = pearsonr(xval_type_41, temp_type_41)
spearman_corr_xval_temp_41, spearman_p_xval_temp_41 = spearmanr(xval_type_41, temp_type_41)

pearson_corr_yval_temp_41, pearson_p_yval_temp_41 = pearsonr(yval_type_41, temp_type_41)
spearman_corr_yval_temp_41, spearman_p_yval_temp_41 = spearmanr(yval_type_41, temp_type_41)

pearson_corr_zval_temp_41, pearson_p_zval_temp_41 = pearsonr(zval_type_41, temp_type_41)
spearman_corr_zval_temp_41, spearman_p_zval_temp_41 = spearmanr(zval_type_41, temp_type_41)

# Compute Pearson and Spearman correlations for type_num = 42
pearson_corr_xval_temp_42, pearson_p_xval_temp_42 = pearsonr(xval_type_42, temp_type_42)
spearman_corr_xval_temp_42, spearman_p_xval_temp_42 = spearmanr(xval_type_42, temp_type_42)

pearson_corr_yval_temp_42, pearson_p_yval_temp_42 = pearsonr(yval_type_42, temp_type_42)
spearman_corr_yval_temp_42, spearman_p_yval_temp_42 = spearmanr(yval_type_42, temp_type_42)

pearson_corr_zval_temp_42, pearson_p_zval_temp_42 = pearsonr(zval_type_42, temp_type_42)
spearman_corr_zval_temp_42, spearman_p_zval_temp_42 = spearmanr(zval_type_42, temp_type_42)

# Print results for type_num = 41
print("For type_num = 41:")
print("Pearson Correlation Results:")
print(f"xval vs temp: Correlation = {pearson_corr_xval_temp_41}, p-value = {pearson_p_xval_temp_41}")
print(f"yval vs temp: Correlation = {pearson_corr_yval_temp_41}, p-value = {pearson_p_yval_temp_41}")
print(f"zval vs temp: Correlation = {pearson_corr_zval_temp_41}, p-value = {pearson_p_zval_temp_41}")

print("\nSpearman Correlation Results:")
print(f"xval vs temp: Correlation = {spearman_corr_xval_temp_41}, p-value = {spearman_p_xval_temp_41}")
print(f"yval vs temp: Correlation = {spearman_corr_yval_temp_41}, p-value = {spearman_p_yval_temp_41}")
print(f"zval vs temp: Correlation = {spearman_corr_zval_temp_41}, p-value = {spearman_p_zval_temp_41}")

# Print results for type_num = 42
print("\nFor type_num = 42:")
print("Pearson Correlation Results:")
print(f"xval vs temp: Correlation = {pearson_corr_xval_temp_42}, p-value = {pearson_p_xval_temp_42}")
print(f"yval vs temp: Correlation = {pearson_corr_yval_temp_42}, p-value = {pearson_p_yval_temp_42}")
print(f"zval vs temp: Correlation = {pearson_corr_zval_temp_42}, p-value = {pearson_p_zval_temp_42}")

print("\nSpearman Correlation Results:")
print(f"xval vs temp: Correlation = {spearman_corr_xval_temp_42}, p-value = {spearman_p_xval_temp_42}")
print(f"yval vs temp: Correlation = {spearman_corr_yval_temp_42}, p-value = {spearman_p_yval_temp_42}")
print(f"zval vs temp: Correlation = {spearman_corr_zval_temp_42}, p-value = {spearman_p_zval_temp_42}")


##########ANALYSIS PRINTING 1:
# # Interpretation function
# def interpret_correlation_results(correlation, p_value, alpha=0.05):
#     if p_value < alpha:
#         if correlation > 0:
#             return f"Significant positive correlation (p < {alpha})"
#         elif correlation < 0:
#             return f"Significant negative correlation (p < {alpha})"
#     return "No significant correlation (p >= 0.05)"

# # Interpretation for type_num = 41
# print("\nInterpretation for type_num = 41:")
# print("xval vs temp:")
# print("Pearson:", interpret_correlation_results(pearson_corr_xval_temp_41, pearson_p_xval_temp_41))
# print("Spearman:", interpret_correlation_results(spearman_corr_xval_temp_41, spearman_p_xval_temp_41))
# print("yval vs temp:")
# print("Pearson:", interpret_correlation_results(pearson_corr_yval_temp_41, pearson_p_yval_temp_41))
# print("Spearman:", interpret_correlation_results(spearman_corr_yval_temp_41, spearman_p_yval_temp_41))
# print("zval vs temp:")
# print("Pearson:", interpret_correlation_results(pearson_corr_zval_temp_41, pearson_p_zval_temp_41))
# print("Spearman:", interpret_correlation_results(spearman_corr_zval_temp_41, spearman_p_zval_temp_41))

# # Interpretation for type_num = 42
# print("\nInterpretation for type_num = 42:")
# print("xval vs temp:")
# print("Pearson:", interpret_correlation_results(pearson_corr_xval_temp_42, pearson_p_xval_temp_42))
# print("Spearman:", interpret_correlation_results(spearman_corr_xval_temp_42, spearman_p_xval_temp_42))
# print("yval vs temp:")
# print("Pearson:", interpret_correlation_results(pearson_corr_yval_temp_42, pearson_p_yval_temp_42))
# print("Spearman:", interpret_correlation_results(spearman_corr_yval_temp_42, spearman_p_yval_temp_42))
# print("zval vs temp:")
# print("Pearson:", interpret_correlation_results(pearson_corr_zval_temp_42, pearson_p_zval_temp_42))
# print("Spearman:", interpret_correlation_results(spearman_corr_zval_temp_42, spearman_p_zval_temp_42))

###############################

##########ANALYSIS PRINTING 2:
# # Interpretation function
# def interpret_correlation_results(correlation, p_value, alpha=0.05):
#     interpretation = ""
#     if p_value < alpha:
#         if correlation > 0:
#             interpretation += "Correlation Coefficient close to +1 indicates a strong positive"
#         elif correlation < 0:
#             interpretation += "Correlation Coefficient close to -1 indicates a strong negative"
#         else:
#             interpretation += "Correlation Coefficient close to 0 indicates no"
        
#         if abs(correlation) >= 0.7:
#             interpretation += " linear relationship."
#         elif abs(correlation) >= 0.3:
#             interpretation += " moderate linear relationship."
#         else:
#             interpretation += " weak linear relationship."
        
#         interpretation += " Statistical significance."
#     else:
#         interpretation += "No statistical significance (p >= 0.05)."
    
#     return interpretation

# # Interpretation for type_num = 41
# print("\nInterpretation for type_num = 41:")
# print("xval vs temp:")
# print("Pearson Correlation:")
# print(interpret_correlation_results(pearson_corr_xval_temp_41, pearson_p_xval_temp_41))
# print("Spearman Correlation:")
# print(interpret_correlation_results(spearman_corr_xval_temp_41, spearman_p_xval_temp_41))

# print("\nyval vs temp:")
# print("Pearson Correlation:")
# print(interpret_correlation_results(pearson_corr_yval_temp_41, pearson_p_yval_temp_41))
# print("Spearman Correlation:")
# print(interpret_correlation_results(spearman_corr_yval_temp_41, spearman_p_yval_temp_41))

# print("\nzval vs temp:")
# print("Pearson Correlation:")
# print(interpret_correlation_results(pearson_corr_zval_temp_41, pearson_p_zval_temp_41))
# print("Spearman Correlation:")
# print(interpret_correlation_results(spearman_corr_zval_temp_41, spearman_p_zval_temp_41))

# # Interpretation for type_num = 42
# print("\nInterpretation for type_num = 42:")
# print("xval vs temp:")
# print("Pearson Correlation:")
# print(interpret_correlation_results(pearson_corr_xval_temp_42, pearson_p_xval_temp_42))
# print("Spearman Correlation:")
# print(interpret_correlation_results(spearman_corr_xval_temp_42, spearman_p_xval_temp_42))

# print("\nyval vs temp:")
# print("Pearson Correlation:")
# print(interpret_correlation_results(pearson_corr_yval_temp_42, pearson_p_yval_temp_42))
# print("Spearman Correlation:")
# print(interpret_correlation_results(spearman_corr_yval_temp_42, spearman_p_yval_temp_42))

# print("\nzval vs temp:")
# print("Pearson Correlation:")
# print(interpret_correlation_results(pearson_corr_zval_temp_42, pearson_p_zval_temp_42))
# print("Spearman Correlation:")
# print(interpret_correlation_results(spearman_corr_zval_temp_42, spearman_p_zval_temp_42))


###############################

##########ANALYSIS PRINTING 3:
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

# Interpretation for type_num = 41
print("\nInterpretation for type_num = 41:")
print("xval vs temp:")
print("Pearson Correlation:")
print(interpret_correlation_results(pearson_corr_xval_temp_41, pearson_p_xval_temp_41))
print("Spearman Correlation:")
print(interpret_correlation_results(spearman_corr_xval_temp_41, spearman_p_xval_temp_41))

print("\nyval vs temp:")
print("Pearson Correlation:")
print(interpret_correlation_results(pearson_corr_yval_temp_41, pearson_p_yval_temp_41))
print("Spearman Correlation:")
print(interpret_correlation_results(spearman_corr_yval_temp_41, spearman_p_yval_temp_41))

print("\nzval vs temp:")
print("Pearson Correlation:")
print(interpret_correlation_results(pearson_corr_zval_temp_41, pearson_p_zval_temp_41))
print("Spearman Correlation:")
print(interpret_correlation_results(spearman_corr_zval_temp_41, spearman_p_zval_temp_41))

# Interpretation for type_num = 42
print("\nInterpretation for type_num = 42:")
print("xval vs temp:")
print("Pearson Correlation:")
print(interpret_correlation_results(pearson_corr_xval_temp_42, pearson_p_xval_temp_42))
print("Spearman Correlation:")
print(interpret_correlation_results(spearman_corr_xval_temp_42, spearman_p_xval_temp_42))

print("\nyval vs temp:")
print("Pearson Correlation:")
print(interpret_correlation_results(pearson_corr_yval_temp_42, pearson_p_yval_temp_42))
print("Spearman Correlation:")
print(interpret_correlation_results(spearman_corr_yval_temp_42, spearman_p_yval_temp_42))

print("\nzval vs temp:")
print("Pearson Correlation:")
print(interpret_correlation_results(pearson_corr_zval_temp_42, pearson_p_zval_temp_42))
print("Spearman Correlation:")
print(interpret_correlation_results(spearman_corr_zval_temp_42, spearman_p_zval_temp_42))





















