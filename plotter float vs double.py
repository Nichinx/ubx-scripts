# -*- coding: utf-8 -*-
"""
Created on Mon Oct  9 17:54:07 2023

@author: nichm
"""

import pandas as pd
from collections import Counter
import matplotlib.pyplot as plt
import numpy as np 
import math
import matplotlib.gridspec as gridspec
import matplotlib.ticker as mticker


data = pd.read_csv('exp11.csv')
data = data.loc[data[data.columns[0]].str.contains('data to send')]

column1_split = data[data.columns[0]].str.split(':', expand=True)
logger_name = column1_split[1]
rtk_fix = column1_split[2]

column8_split = data[data.columns[8]].str.split('*', expand=True)
rtc_volt = column8_split[0]
ts = column8_split[1]

data_list = list(zip(logger_name,ts,rtk_fix,data[data.columns[1]],
                                            data[data.columns[2]],
                                            data[data.columns[3]],
                                            data[data.columns[4]],
                                            data[data.columns[5]],
                                            data[data.columns[6]],
                                            data[data.columns[7]], rtc_volt))

new_df = pd.DataFrame(data_list, columns = ['logger',
                                            'ts', 
                                            'fix',
                                            'lat',
                                            'lon',
                                            'hacc',
                                            'vacc',
                                            'msl',
                                            'sat_num',
                                            'temp',
                                            'volt'])
new_df = new_df.loc[(new_df.fix == '2')]


df1 = new_df.loc[~(new_df.logger.str.contains("double"))]
df2 = new_df.loc[(new_df.logger.str.contains("double"))]


#LAT LON DISP PLOTTER

#df1
df1['ts'] = pd.to_datetime(df1['ts'], format='%y%m%d%H%M%S')
df1['lat'] = pd.to_numeric(df1['lat'])
df1['lon'] = pd.to_numeric(df1['lon'])
df1['hacc'] = pd.to_numeric(df1['hacc'])
df1['vacc'] = pd.to_numeric(df1['vacc'])
df1['msl'] = pd.to_numeric(df1['msl'])

df1_copy = df1[['ts']].copy()
df1_copy['lat2'] = df1.lat
df1_copy['lon2'] = df1.lon
df1_copy['lat1'] = df1_copy.lat2.shift(-1)
df1_copy['lon1'] = df1_copy.lon2.shift(-1)

df1_phi1 = df1_copy.lat1 * math.pi/180
df1_phi2 = df1_copy.lat2 * math.pi/180

df1_copy['delta_lat'] = (df1_copy.lat2 - df1_copy.lat1) * math.pi/180
df1_copy['delta_lon'] = (df1_copy.lon2 - df1_copy.lon1) * math.pi/180
df1_copy= df1_copy.fillna(0)

#HAVERSINE
radius_earth = 6371e3
haver_df1 = np.sin(df1_copy.delta_lat/2)**2 + \
            np.cos(df1_copy.lat1) * \
            np.cos(df1_copy.lat2) * \
            np.sin(df1_copy.delta_lon/2)**2
sqhvr_df1 = haver_df1**(1/2)
sqhvrmin1_df1 = (1-haver_df1)**(1/2)
circ_df1 = 2 * np.arctan2(sqhvr_df1, sqhvrmin1_df1)
dist_df1 = radius_earth * circ_df1     #in meters
df1_copy['dist'] = dist_df1


#df2
df2['ts'] = pd.to_datetime(df2['ts'], format='%y%m%d%H%M%S')
df2['lat'] = pd.to_numeric(df2['lat'])
df2['lon'] = pd.to_numeric(df2['lon'])
df2['hacc'] = pd.to_numeric(df2['hacc'])
df2['vacc'] = pd.to_numeric(df2['vacc'])
df2['msl'] = pd.to_numeric(df2['msl'])

df2_copy = df2[['ts']].copy()
df2_copy['lat2'] = df2.lat
df2_copy['lon2'] = df2.lon
df2_copy['lat1'] = df2_copy.lat2.shift(-1)
df2_copy['lon1'] = df2_copy.lon2.shift(-1)

df2_phi1 = df2_copy.lat1 * math.pi/180
df2_phi2 = df2_copy.lat2 * math.pi/180

df2_copy['delta_lat'] = (df2_copy.lat2 - df2_copy.lat1) * math.pi/180
df2_copy['delta_lon'] = (df2_copy.lon2 - df2_copy.lon1) * math.pi/180
df2_copy= df2_copy.fillna(0)

#HAVERSINE
haver_df2 = np.sin(df2_copy.delta_lat/2)**2 + \
            np.cos(df2_copy.lat1) * \
            np.cos(df2_copy.lat2) * \
            np.sin(df2_copy.delta_lon/2)**2
sqhvr_df2 = haver_df2**(1/2)
sqhvrmin1_df2 = (1-haver_df2)**(1/2)
circ_df2 = 2 * np.arctan2(sqhvr_df2, sqhvrmin1_df2)
dist_df2 = radius_earth * circ_df2     #in meters
df2_copy['dist'] = dist_df2



#PLOT
fig = plt.figure()
fig.suptitle('Moving Dist', fontweight='bold')
gs = gridspec.GridSpec(3, 2) #3 by 2 subplot


#df1:
plt.subplot(gs[0,0])
plt.plot(df1_copy.ts, df1_copy.lat2, "green")
plt.ylabel('latitude, °', fontweight='bold')
plt.grid(color = 'gray', linestyle = '--', linewidth = 0.5)
plt.xticks(color='w')

plt.subplot(gs[1,0])
plt.plot(df1_copy.ts, df1_copy.lon2, "blue")
plt.ylabel('longitude, °', fontweight='bold')
plt.grid(color = 'gray', linestyle = '--', linewidth = 0.5)
plt.xticks(color='w')

plt.subplot(gs[2,0])
plt.plot(df1_copy.ts, df1_copy.dist, "red")
plt.ylabel('disdft, m', fontweight='bold')
plt.grid(color = 'gray', linestyle = '--', linewidth = 0.5)
plt.xticks(rotation=90)

#df2:
plt.subplot(gs[0,1])
plt.plot(df2_copy.ts, df2_copy.lat2, "green")
plt.ylabel('latitude, °', fontweight='bold')
plt.grid(color = 'gray', linestyle = '--', linewidth = 0.5)
plt.xticks(color='w')

plt.subplot(gs[1,1])
plt.plot(df2_copy.ts, df2_copy.lon2, "blue")
plt.ylabel('longitude, °', fontweight='bold')
plt.grid(color = 'gray', linestyle = '--', linewidth = 0.5)
plt.xticks(color='w')

plt.subplot(gs[2,1])
plt.plot(df2_copy.ts, df2_copy.dist, "red")
plt.ylabel('dist, m', fontweight='bold')
plt.grid(color = 'gray', linestyle = '--', linewidth = 0.5)
plt.xticks(rotation=90)


plt.show()

