import pandas as pd
from collections import Counter
import matplotlib.pyplot as plt
import numpy as np 
import math
import matplotlib.gridspec as gridspec
import matplotlib.ticker as mticker
from matplotlib.ticker import FormatStrFormatter
from matplotlib.ticker import StrMethodFormatter
import utm


#CSV READER
data = pd.read_csv('exp23.csv')
# data = data.loc[data[data.columns[0]].str.contains('data to send')] #data string row finder

column1_split = data[data.columns[0]].str.split(':', expand=True)
logger_name = column1_split[0]
rtk_fix = column1_split[1]

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
new_df['msl'] = np.round(new_df.msl,2)
# new_df = new_df.loc[(new_df.fix == '2') & (new_df.sat_num > 28)]
df2 = new_df.loc[~(np.isnan(new_df.lat))].reset_index(drop=True)

# df = utm.from_latlon(new_df.lat.to_numpy(), new_df.lon.to_numpy())
# df = pd.DataFrame([list(df)]).transpose()
# df2['lat_UTM'] = df.loc[0].explode(list()).reset_index(drop=True)
# df2['lon_UTM'] = df.loc[1].explode(list()).reset_index(drop=True)
# # df2.insert(2, 'zone#', df.loc[2])
# # df2.insert(3, 'zoneL', df.loc[3])


df = utm.from_latlon(df2.lat.to_numpy(), df2.lon.to_numpy())
df = pd.DataFrame([list(df)]).transpose()
df2['lat_UTM'] = df.loc[0].explode(list()).reset_index(drop=True)
df2['lon_UTM'] = df.loc[1].explode(list()).reset_index(drop=True)
# df2.insert(2, 'zone#', df.loc[2])
# df2.insert(3, 'zoneL', df.loc[3])



#LAT LON DISP PLOTTER
#df2
df2['ts'] = pd.to_datetime(df2['ts'], format='%y%m%d%H%M%S')
df2['lat'] = pd.to_numeric(df2['lat'])
df2['lon'] = pd.to_numeric(df2['lon'])
df2['hacc'] = pd.to_numeric(df2['hacc'])
df2['vacc'] = pd.to_numeric(df2['vacc'])
df2['msl'] = pd.to_numeric(df2['msl'])
# df2['lat_UTM'] = pd.to_numeric(df2['lat_UTM'])
# df2['lon_UTM'] = pd.to_numeric(df2['lon_UTM'])

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
radius_earth = 6371e3
haver_df2 = np.sin(df2_copy.delta_lat/2)**2 + \
            np.cos(df2_copy.lat1) * \
            np.cos(df2_copy.lat2) * \
            np.sin(df2_copy.delta_lon/2)**2
sqhvr_df2 = haver_df2**(1/2)
sqhvrmin1_df2 = (1-haver_df2)**(1/2)
circ_df2 = 2 * np.arctan2(sqhvr_df2, sqhvrmin1_df2)
dist_df2 = radius_earth * circ_df2     #in meters
df2_copy['dist'] = dist_df2


#moving average
window_size = 6

wind_lat2 = df2_copy.lat2.rolling(window_size)
mva_lat2 = wind_lat2.mean()
mva_lat2_list = mva_lat2.tolist()
mva_lat2_final_list = mva_lat2_list[window_size - 1:]

wind_lon2 = df2_copy.lon2.rolling(window_size)
mva_lon2 = wind_lon2.mean()
mva_lon2_list = mva_lon2.tolist()
mva_lon2_final_list = mva_lon2_list[window_size - 1:]

wind_msl= df2.msl.rolling(window_size)
mva_msl = wind_msl.mean()
mva_msl_list = mva_msl.tolist()
mva_msl_final_list = mva_msl_list[window_size - 1:]

wind_dist= df2_copy.dist.rolling(window_size)
mva_dist = wind_dist.mean()
mva_dist_list = mva_dist.tolist()
mva_dist_final_list = mva_dist_list[window_size - 1:]

df_new_mva  = df2_copy[['ts']].copy()
df_new_mva.drop(df_new_mva.head((window_size - 1)).index,inplace=True)
df_new_mva['mva_lat2'] = mva_lat2_final_list
df_new_mva['mva_lon2'] = mva_lon2_final_list
df_new_mva['mva_msl'] = mva_msl_final_list
df_new_mva['mva_dist'] = mva_dist_final_list


#PLOT
fig = plt.figure()
# fig.suptitle('rover in stable position - averaging 12 data points with filter', fontweight='bold')
gs = gridspec.GridSpec(4, 1) #4 by 1 subplot

#df2:
plt.subplot(gs[0,0])
plt.plot(df2.ts, df2.lat_UTM, "blue")
plt.gca().yaxis.set_major_formatter(FormatStrFormatter('%f'))
# plt.plot(df2_copy.ts, df2_copy.lat2, "green", alpha=0.5)
# plt.plot(df_new_mva.ts,df_new_mva.mva_lat2, "blue")
plt.ylabel('latitude, °', fontweight='bold')
plt.grid(color = 'gray', linestyle = '--', linewidth = 0.5)
plt.xticks(color='w')

plt.subplot(gs[1,0])
plt.plot(df2.ts, df2.lon_UTM, "blue")
plt.gca().yaxis.set_major_formatter(FormatStrFormatter('%f'))
# plt.plot(df2_copy.ts, df2_copy.lon2, "green", alpha=0.5)
# plt.plot(df_new_mva.ts,df_new_mva.mva_lon2, "blue")
plt.ylabel('longitude, °', fontweight='bold')
plt.grid(color = 'gray', linestyle = '--', linewidth = 0.5)
plt.xticks(color='w')

plt.subplot(gs[2,0])
plt.plot(df2.ts, df2.msl, "violet", alpha=0.5)
plt.plot(df_new_mva.ts,df_new_mva.mva_msl, "red")
plt.ylabel('msl, m', fontweight='bold')
plt.grid(color = 'gray', linestyle = '--', linewidth = 0.5)

plt.subplot(gs[3,0])
plt.plot(df2_copy.ts, df2_copy.dist, "orange", alpha=0.5)
plt.plot(df_new_mva.ts,df_new_mva.mva_dist, "red")
plt.ylabel('dist, m', fontweight='bold')
plt.grid(color = 'gray', linestyle = '--', linewidth = 0.5)
plt.xticks(rotation=90)

plt.show()



#UTM PLOT
# fig = plt.figure()
# fig.suptitle('UTM plot', fontweight='bold')
# gs = gridspec.GridSpec(2, 1) #4 by 1 subplot

# #df2:
# plt.subplot(gs[0,0])
# plt.plot(df2.ts, df2.lat_UTM, "red")
# plt.ylabel('lat, m', fontweight='bold')
# plt.grid(color = 'gray', linestyle = '--', linewidth = 0.5)
# plt.xticks(rotation=90)
# plt.gca().yaxis.set_major_formatter(FormatStrFormatter('%.6f'))

# plt.subplot(gs[1,0])
# plt.plot(df2.ts, df2.lon_UTM, "blue")
# plt.ylabel('lon, m', fontweight='bold')
# plt.grid(color = 'gray', linestyle = '--', linewidth = 0.5)
# plt.gca().yaxis.set_major_formatter(FormatStrFormatter('%.6f'))
# plt.xticks(rotation=90)

# plt.show()