# -*- coding: utf-8 -*-
"""
Created on Mon Sep  4 18:00:18 2023

@author: Ket
"""

import pandas as pd
from collections import Counter
import matplotlib.pyplot as plt
import numpy as np 
import math
import matplotlib.gridspec as gridspec


data = pd.read_csv('ublox_data_no_correction.csv')


column1_split = data[data.columns[0]].str.split(':', expand=True)
logger_name = column1_split[0]
rtk_fix = column1_split[1]

column8_split = data[data.columns[8]].str.split('*', expand=True)
rtc_volt = column8_split[0]
ts = column8_split[1]

data_list = list(zip(ts,rtk_fix,data[data.columns[1]],
                                data[data.columns[2]],
                                data[data.columns[3]],
                                data[data.columns[4]],
                                data[data.columns[5]],
                                data[data.columns[6]],
                                data[data.columns[7]], rtc_volt))

new_df = pd.DataFrame(data_list, columns = ['ts', 
                                            'fix',
                                            'lat',
                                            'lon',
                                            'hacc',
                                            'vacc',
                                            'msl',
                                            'sat_num',
                                            'temp',
                                            'volt'])


#FREQUENCY COUNTER
lat_lon_list = list(zip(new_df.lat, new_df.lon))
lat_lon_counter = Counter(lat_lon_list)
df_lat_lon_counter = pd.DataFrame.from_dict(lat_lon_counter, 
                                            orient='index').reset_index()
df_lat_lon_counter.columns = ['lat_lon','freq']
df_lat_lon_counter = df_lat_lon_counter.reset_index(drop=True)
print(df_lat_lon_counter)
 
lat_long_freq_filtered = df_lat_lon_counter.loc[(df_lat_lon_counter.freq != 1)]
print(lat_long_freq_filtered)

#LAT LON DISP PLOTTER
new_df['ts'] = pd.to_datetime(new_df['ts'], format='%y%m%d%H%M%S')
new_df['lat'] = pd.to_numeric(new_df['lat'])
new_df['lon'] = pd.to_numeric(new_df['lon'])
new_df['hacc'] = pd.to_numeric(new_df['hacc'])
new_df['vacc'] = pd.to_numeric(new_df['vacc'])
new_df['msl'] = pd.to_numeric(new_df['msl'])
# new_df['temp'] = pd.to_numeric(new_df['temp'], errors='coerce').isnull()
# new_df['volt'] = pd.to_numeric(new_df['volt'])


new_df_copy = new_df[['ts']].copy()
new_df_copy['lat2'] = new_df.lat
new_df_copy['lon2'] = new_df.lon
new_df_copy['lat1'] = new_df_copy.lat2.shift(-1)
new_df_copy['lon1'] = new_df_copy.lon2.shift(-1)

phi1 = new_df_copy.lat1 * math.pi/180
phi2 = new_df_copy.lat2 * math.pi/180

new_df_copy['delta_lat'] = (new_df_copy.lat2 - new_df_copy.lat1) * math.pi/180
new_df_copy['delta_lon'] = (new_df_copy.lon2 - new_df_copy.lon1) * math.pi/180
new_df_copy= new_df_copy.fillna(0)

#HAVERSINE
radius_earth = 6371e3
haver = np.sin(new_df_copy.delta_lat/2)**2 + \
        np.cos(new_df_copy.lat1) * \
        np.cos(new_df_copy.lat2) * \
        np.sin(new_df_copy.delta_lon/2)**2
sqhvr = haver**(1/2)
sqhvrmin1 = (1-haver)**(1/2)
circ = 2 * np.arctan2(sqhvr, sqhvrmin1)
dist = radius_earth * circ     #in meters
new_df_copy['dist'] = dist


fig = plt.figure()
fig.suptitle('longitude vs. latitude : TESUA (no correction data)', fontweight='bold')
gs = gridspec.GridSpec(3, 1) #3 by 1 subplot

#3 by 1 subplot
plt.subplot(gs[0,0])
plt.plot(new_df_copy.ts, new_df_copy.lat2, "green")
# plt.plot(df_new_mva.ts,df_new_mva.mva_lat2)
plt.ylabel('latitude, °', fontweight='bold')
plt.grid(color = 'gray', linestyle = '--', linewidth = 0.5)
plt.xticks(color='w')

plt.subplot(gs[1,0])
plt.plot(new_df_copy.ts, new_df_copy.lon2, "blue")
# plt.plot(df_new_mva.ts,df_new_mva.mva_lon2)
plt.ylabel('longitude, °', fontweight='bold')
plt.grid(color = 'gray', linestyle = '--', linewidth = 0.5)
plt.xticks(color='w')

plt.subplot(gs[2,0])
plt.plot(new_df_copy.ts, new_df_copy.dist, "red")
plt.ylabel('dist, m', fontweight='bold')
plt.grid(color = 'gray', linestyle = '--', linewidth = 0.5)
plt.xticks(rotation=90)

plt.show()


#HISTOGRAM
def addlabels(x,y):
    for i in range(len(x)):
        plt.text(i, y[i], y[i], ha='center')
    
x_axis = lat_long_freq_filtered.lat_lon
x_axis_array = np.stack(x_axis).astype(None)
y_axis = (lat_long_freq_filtered.freq/len(new_df.ts))*100
y_axis = np.round(y_axis,2)
ax = lat_long_freq_filtered.plot(kind="bar", width=0.15, align='center')

ax.set_title("Position coordinate data vs Frequency - no correction data")
ax.set_xticklabels(x_axis_array)
# addlabels(x_axis,y_axis)