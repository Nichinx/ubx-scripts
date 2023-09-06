import mysql.connector
import pandas as pd
import numpy as np 
import math
from collections import Counter
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from datetime import datetime, timedelta

pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.max_column', False)
pd.options.display.float_format = "{:,}".format

db = mysql.connector.connect(
    host="192.168.150.112",
    user="pysys_local",
    password="NaCAhztBgYZ3HwTkvHwwGVtJn5sVMFgg",
    database="analysis_db")

# query = "SELECT * FROM analysis_db.ublox_sinua  ORDER BY ts desc" #old sinua query
query = "SELECT * FROM analysis_db.gnss_sinsa where ts > '2022-07-27'" #new sinua query
# query = "SELECT * FROM analysis_db.gnss_testa  ORDER BY ts desc" #testa query

data = pd.read_sql(query,db)
data = data.sort_values(by='ts', ascending=True, ignore_index=True)
###old sinua query
# data_filtered = data.loc[(data['prec'] == 0.0141)].reset_index(drop=True)\
#     .sort_values(by='ts', ascending=True, ignore_index=True)
# data_filtered = data.loc[(data['accuracy'] == 0.0141)].reset_index(drop=True)\
#     .sort_values(by='ts', ascending=True, ignore_index=True)    
###new sinua query | tesua query
data_filtered = data.loc[(data['accuracy'] == 0.0141) & \
    (data['fix_type'] == 2)].reset_index(drop=True)\
    .sort_values(by='ts', ascending=True, ignore_index=True)

data['ts'] = pd.to_datetime(data['ts'])
data['latitude'] = pd.to_numeric(data['latitude'])
data['longitude'] = pd.to_numeric(data['longitude'])
data['accuracy'] = pd.to_numeric(data['accuracy'])
data['msl'] = pd.to_numeric(data['msl'])
data['volt'] = pd.to_numeric(data['volt'])

df = data[['ts','fix_type','latitude','longitude','accuracy']].copy()
df['msl_round'] = np.round(data.msl,2)

post_zip = list(df.msl_round)
counter_post = Counter(post_zip) 
cp_df = pd.DataFrame.from_dict(counter_post, orient='index').reset_index()
cp_df.columns = ['msl_round','freq']
cp_df_top_ri = cp_df.reset_index(drop=True)

new_df = pd.merge(df, cp_df_top_ri, on='msl_round')
#filter by frequency count
new_df = new_df.loc[(new_df.freq >= (np.round((cp_df_top_ri.freq.max() * .25),2)))].sort_values(by='ts', ascending=True, ignore_index=True)

#25% on frequency
# msl_tol_max = np.round((cp_df_top_ri.freq.max() * 1.05),2)
# msl_tol_min = np.round((cp_df_top_ri.freq.max() * 0.95),2)
# cp_df_top_ri.loc[(cp_df_top_ri.freq >= (np.round((cp_df_top_ri.freq.max() * .25),2)))]



# # # #0.5% tolerance on MSL
# msl_tol_max = np.round((cp_df_top_ri.msl_round.max() * 1.005),2)
# msl_tol_min = np.round((cp_df_top_ri.msl_round.max() * 0.995),2)
# # msl_tol_max = np.round((cp_df_top_ri.msl_round.max() + 1),2)
# # msl_tol_min = np.round((cp_df_top_ri.msl_round.max() - 1),2)

# new_df_on_tol = df.loc[(df['msl_round'] > msl_tol_min) & (df['msl_round'] < msl_tol_max)]
# # # cp_df_top_ri.freq.loc[(cp_df_top_ri.freq >= 1)].sum()
# cp_df_top_ri.loc[(cp_df_top_ri.freq >= (np.round((cp_df_top_ri.freq.max() * .25),2)))]

# new_df_on_tol = df.loc[(df['msl_round'] > msl_tol_min) & (df['msl_round'] < msl_tol_max)]




###################PLOT
radius_earth = 6371e3

df_forplot = new_df[['ts']].copy()
df_forplot['lat2'] = new_df.latitude
df_forplot['lon2'] = new_df.longitude
df_forplot['lat1'] = df_forplot.lat2.shift(-1)
df_forplot['lon1'] = df_forplot.lon2.shift(-1)

# df_forplot = new_df_on_tol[['ts']].copy()
# df_forplot['lat2'] = new_df_on_tol.latitude
# df_forplot['lon2'] = new_df_on_tol.longitude
# df_forplot['lat1'] = df_forplot.lat2.shift(-1)
# df_forplot['lon1'] = df_forplot.lon2.shift(-1)

phi1 = df_forplot.lat1 * math.pi/180
phi2 = df_forplot.lat2 * math.pi/180

df_forplot['delta_lat'] = (df_forplot.lat2 - df_forplot.lat1) * math.pi/180
df_forplot['delta_lon'] = (df_forplot.lon2 - df_forplot.lon1) * math.pi/180
df_forplot= df_forplot.fillna(0)

#haversine
haver = np.sin(df_forplot.delta_lat/2)**2 + np.cos(df_forplot.lat1) * np.cos(df_forplot.lat2) * np.sin(df_forplot.delta_lon/2)**2
sqhvr = haver**(1/2)
sqhvrmin1 = (1-haver)**(1/2)
circ = 2 * np.arctan2(sqhvr, sqhvrmin1)
dist = radius_earth * circ     #in meters
df_forplot['dist'] = dist


#moving average
window_size = 11 

wind_lat2 = df_forplot.lat2.rolling(window_size)
mva_lat2 = wind_lat2.mean()
mva_lat2_list = mva_lat2.tolist()
mva_lat2_final_list = mva_lat2_list[window_size - 1:]

wind_lon2 = df_forplot.lon2.rolling(window_size)
mva_lon2 = wind_lon2.mean()
mva_lon2_list = mva_lon2.tolist()
mva_lon2_final_list = mva_lon2_list[window_size - 1:]


df_new_mva  = df_forplot[['ts']].copy()
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
plt.plot(df_forplot.ts, df_forplot.lat2, "green", alpha=0.5)
plt.plot(df_new_mva.ts,df_new_mva.mva_lat2)
plt.ylabel('latitude, °', fontweight='bold')
plt.grid(color = 'gray', linestyle = '--', linewidth = 0.5)
plt.xticks(color='w')

plt.subplot(gs[0,1])
plt.plot(df_forplot.ts, df_forplot.lon2, "blue", alpha=0.5)
plt.plot(df_new_mva.ts,df_new_mva.mva_lon2)
plt.ylabel('longitude, °', fontweight='bold')
plt.grid(color = 'gray', linestyle = '--', linewidth = 0.5)
plt.xticks(color='w')

plt.subplot(gs[1,:])
plt.plot(df_forplot.ts, df_forplot.dist, "red")
plt.ylabel('dist, m', fontweight='bold')
plt.grid(color = 'gray', linestyle = '--', linewidth = 0.5)
plt.xticks(rotation=90)


figManager = plt.get_current_fig_manager()
figManager.window.showMaximized()
plt.show()
