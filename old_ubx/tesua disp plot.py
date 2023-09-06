import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import numpy as np 
import math
from textwrap import wrap
# from mpl_toolkits.basemapt import Basemap


pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.max_column', None)
# pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_rows', None)
# pd.options.display.float_format = "{:,.9f}".format


db = mysql.connector.connect(
    host="192.168.150.112",
    user="pysys_local",
    password="NaCAhztBgYZ3HwTkvHwwGVtJn5sVMFgg",
    database="analysis_db")
query = "SELECT * FROM analysis_db.gnss_sinsa where ts > '2022-07-27'"


# query = "SELECT * FROM new_db.ubx_tesua \
#         UNION ALL \
#         SELECT ts, logger_id, fix_type, latitude, longitude, accuracy, msl, NULL, volt \
#         FROM new_db.ubx_tesua_notemp \
#         ORDER BY ts"
        
# selecting 1.41cm acc only
# query = "SELECT * FROM new_db.ubx_tesua \
#         WHERE accuracy=0.0141 \
#         UNION ALL \
#         SELECT ts, logger_id, fix_type, latitude, longitude, accuracy, msl, NULL, volt \
#         FROM new_db.ubx_tesua_notemp \
#         WHERE accuracy=0.0141 \
#         ORDER BY ts"


data = pd.read_sql(query,db)
# data = data.sort_values(by='ts', ascending=True, ignore_index=True)
data = data.loc[(data['accuracy'] == 0.0141)].reset_index(drop=True).sort_values(by='ts', ascending=True, ignore_index=True)

data['ts'] = pd.to_datetime(data['ts'])
data['latitude'] = pd.to_numeric(data['latitude'])
data['longitude'] = pd.to_numeric(data['longitude'])
# data['accuracy'] = pd.to_numeric(data['accuracy'])
data['msl'] = pd.to_numeric(data['msl'])
# data['temp'] = pd.to_numeric(data['temp'], errors='coerce').isnull()
data['volt'] = pd.to_numeric(data['volt'])



radius_earth = 6371e3

df_new = data[['ts']].copy()
df_new['lat2'] = data.latitude
df_new['lon2'] = data.longitude
df_new['lat1'] = df_new.lat2.shift(-1)
df_new['lon1'] = df_new.lon2.shift(-1)

phi1 = df_new.lat1 * math.pi/180
phi2 = df_new.lat2 * math.pi/180

df_new['delta_lat'] = (df_new.lat2 - df_new.lat1) * math.pi/180
df_new['delta_lon'] = (df_new.lon2 - df_new.lon1) * math.pi/180
df_new= df_new.fillna(0)

#haversine
haver = np.sin(df_new.delta_lat/2)**2 + np.cos(df_new.lat1) * np.cos(df_new.lat2) * np.sin(df_new.delta_lon/2)**2
sqhvr = haver**(1/2)
sqhvrmin1 = (1-haver)**(1/2)
circ = 2 * np.arctan2(sqhvr, sqhvrmin1)
dist = radius_earth * circ     #in meters
df_new['dist'] = dist



#moving average -- alt
# i = -1
# window_size = 3
# moving_averages = []
# while i < len(df_new.lat2) - window_size + 1:
#     window_average = round(np.sum(df_new.lat2[
#       i:i+window_size]) / window_size, 2)
#     moving_averages.append(window_average)
#     i += 1



#moving average
window_size = 11 

wind_lat2 = df_new.lat2.rolling(window_size)
mva_lat2 = wind_lat2.mean()
mva_lat2_list = mva_lat2.tolist()
mva_lat2_final_list = mva_lat2_list[window_size - 1:]

wind_lon2 = df_new.lon2.rolling(window_size)
mva_lon2 = wind_lon2.mean()
mva_lon2_list = mva_lon2.tolist()
mva_lon2_final_list = mva_lon2_list[window_size - 1:]


df_new_mva  = df_new[['ts']].copy()
df_new_mva.drop(df_new_mva.head(5).index,inplace=True)   #remove head = n : depends on window
df_new_mva.drop(df_new_mva.tail(5).index,inplace=True)
df_new_mva['mva_lat2'] = mva_lat2_final_list
df_new_mva['mva_lon2'] = mva_lon2_final_list




fig = plt.figure()
# fig.suptitle('longitude vs. latitude : TESUA displacement plot', fontweight='bold')
gs = gridspec.GridSpec(2, 2)
# labels = df_new.ts
# labels = ['\n'.join(wrap(l, 11)) for l in labels]

plt.subplot(gs[0,0])
plt.plot(df_new.ts, df_new.lat2, "green", alpha=0.5)
plt.plot(df_new_mva.ts,df_new_mva.mva_lat2)
plt.ylabel('latitude, °', fontweight='bold')
plt.grid(color = 'gray', linestyle = '--', linewidth = 0.5)
plt.xticks(color='w')

plt.subplot(gs[0,1])
plt.plot(df_new.ts, df_new.lon2, "blue", alpha=0.5)
plt.plot(df_new_mva.ts,df_new_mva.mva_lon2)
plt.ylabel('longitude, °', fontweight='bold')
plt.grid(color = 'gray', linestyle = '--', linewidth = 0.5)
plt.xticks(color='w')

plt.subplot(gs[1,:])
plt.plot(df_new.ts, df_new.dist, "red")
plt.ylabel('dist, m', fontweight='bold')
plt.grid(color = 'gray', linestyle = '--', linewidth = 0.5)
plt.xticks(rotation=90)


figManager = plt.get_current_fig_manager()
figManager.window.showMaximized()
plt.show()