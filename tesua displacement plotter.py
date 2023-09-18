import math
import time
import numpy as np 
import pandas as pd
import mysql.connector
import matplotlib.pyplot as plt
from collections import Counter
from mysql.connector import Error
import matplotlib.gridspec as gridspec
from datetime import datetime, timedelta

import warnings
warnings.filterwarnings("ignore")

try:
    connection = mysql.connector.connect(
                        host='192.168.150.112',
                        user='pysys_local',
                        password='NaCAhztBgYZ3HwTkvHwwGVtJn5sVMFgg',
                        database='analysis_db')
    
    if connection.is_connected():
        db_Info = connection.get_server_info()
        print("Connected to MySQL Server version ", db_Info)
        cursor = connection.cursor()
        cursor.execute("select database();")
        record = cursor.fetchone()
        print("You're connected to database: ", record)

except Error as e:
        print("Error while connecting to MySQL", e)
        

query = "SELECT * FROM analysis_db.gnss_testa"

sql_data = pd.read_sql(query,connection)
data = pd.DataFrame(sql_data)
data.reset_index()


#timestamp / duration
ts_select = input("start or end? : ")

while ((ts_select != "start") or (ts_select != "end")):
    if ts_select==str('end'):
        end_ts = datetime.strptime(input('end ts (YYYY-mm-dd HH:MM:SS): '), 
                               '%Y-%m-%d %H:%M:%S')
        start_ts = (end_ts - timedelta(hours=int(input('time difference (in hours): ')
                                             ))).strftime('%Y-%m-%d %H:%M:%S')
        break

    if ts_select==str('start'):
        start_ts = datetime.strptime(input('start ts (YYYY-mm-dd HH:MM:SS): '), 
                               '%Y-%m-%d %H:%M:%S')
        end_ts = (start_ts + timedelta(hours=int(input('time difference (in hours): ')
                                             ))).strftime('%Y-%m-%d %H:%M:%S')
        break

    print("choose: 'start' or 'end'")
    break
print('')


horizontal_accuracy = 0.0141
vertical_accuracy = 0.01

df_accuracy = data.loc[((data['hacc'] == horizontal_accuracy) & 
                          (data['vacc'] == vertical_accuracy) & 
                          ((data['ts'] >= start_ts) & 
                            (data['ts'] <= end_ts)))].\
                    reset_index(drop=True).\
                    sort_values(by='ts', ascending=True, ignore_index=True)
print('data_accuracy = ',len(df_accuracy))                    
                    
data_complete_decimal = df_accuracy[df_accuracy['latitude'].\
                                        astype(str).str[-10].eq('.')
                                    & df_accuracy['longitude'].\
                                        astype(str).str[-10].eq('.')]
# print('data_complete_decimal = ',len(data_complete_decimal))

df = data_complete_decimal[['ts',   
                            'fix_type',
                            'latitude',
                            'longitude',
                            'hacc',
                            'vacc',]].copy()
df['msl_rounded'] = np.round(data_complete_decimal.msl,2)
print('df = ',len(df))


#MSL frequency counter
msl_zip = list(df.msl_rounded)
counter_msl_zip = Counter(msl_zip)
df_counter_msl_zip = pd.DataFrame.from_dict(counter_msl_zip,
                                            orient='index').reset_index()
df_counter_msl_zip.columns = ['msl_rounded','freq']
df_counter_msl_zip = df_counter_msl_zip.reset_index(drop=True)
new_df_msl_filtered = pd.merge(df, df_counter_msl_zip, on='msl_rounded').\
            sort_values(by='ts', ascending=True, ignore_index=True)

msl_freq_filtered_df = new_df_msl_filtered.loc[(new_df_msl_filtered.freq != 1)].\
                      sort_values(by='ts', ascending=True, ignore_index=True)
# print('msl_freq_filtered_df = ',len(msl_freq_filtered_df))   


# #LATLON frequency counter
# latlon_zip = list(zip(msl_freq_filtered_df.latitude, 
#                       msl_freq_filtered_df.longitude))
# counter_latlon_zip = Counter(latlon_zip)
# df_counter_latlon_zip = pd.DataFrame.from_dict(counter_latlon_zip, 
#                                            orient='index').reset_index()
# df_counter_latlon_zip.columns = ['latlon','freq']
# df_counter_latlon_zip = df_counter_latlon_zip.reset_index(drop=True)



# ##outlier
def outlier_filter(df):
    dff = df.copy()

    dfmean = dff[['latitude','longitude','msl_rounded']].\
            rolling(min_periods=1,window=6,center=False).mean()
    dfsd = dff[['latitude','longitude','msl_rounded']].\
            rolling(min_periods=1,window=6,center=False).std()

    dfulimits = dfmean + (1*dfsd)
    dfllimits = dfmean - (1*dfsd)

    dff.latitude[(dff.latitude > dfulimits.latitude) | \
        (dff.latitude < dfllimits.latitude)] = np.nan
    dff.longitude[(dff.longitude > dfulimits.longitude) | \
        (dff.longitude < dfllimits.longitude)] = np.nan
    dff.msl_rounded[(dff.msl_rounded > dfulimits.msl_rounded) | \
        (dff.msl_rounded < dfllimits.msl_rounded)] = np.nan

    dflogic = dff.latitude * dff.longitude * dff.msl_rounded
    dff = dff[dflogic.notnull()]

    return dff

df = outlier_filter(df)
print('df with outlier = ',len(df))



#LAT LON DISP PLOTTER
df['ts'] = pd.to_datetime(df['ts'], format='%y%m%d%H%M%S')
df['latitude'] = pd.to_numeric(df['latitude'])
df['longitude'] = pd.to_numeric(df['longitude'])
df['hacc'] = pd.to_numeric(df['hacc'])
df['vacc'] = pd.to_numeric(df['vacc'])
df['msl_rounded'] = pd.to_numeric(df['msl_rounded'])


new_df_copy = df[['ts']].copy()
new_df_copy['lat2'] = df.latitude
new_df_copy['lon2'] = df.longitude
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
fig.suptitle('longitude vs. latitude : TESUA (filtered)', fontweight='bold')
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




###############################################################################
fig = plt.figure()
fig.suptitle('msl : unfiltered vs filtered', fontweight='bold')
gs = gridspec.GridSpec(3, 1)

plt.subplot(gs[0,0])
plt.plot(data.ts, data.msl, "red")
plt.ylabel('msl, m', fontweight='bold')
plt.grid(color = 'gray', linestyle = '--', linewidth = 0.5)

plt.subplot(gs[1,0])
plt.plot(df.ts, df.msl_rounded, "blue")
plt.ylabel('msl, m', fontweight='bold')
plt.grid(color = 'gray', linestyle = '--', linewidth = 0.5)

plt.subplot(gs[2,0])
plt.plot(df1.ts, df1.msl_rounded, "green")
plt.ylabel('msl, m', fontweight='bold')
plt.grid(color = 'gray', linestyle = '--', linewidth = 0.5)
current_values1 = plt.gca().get_yticks()
plt.gca().set_yticklabels(['{:.2f}'.format(x) for x in current_values1])
plt.setp(ax.get_yticklabels(), rotation=0, ha="center")









