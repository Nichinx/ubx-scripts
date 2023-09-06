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

#start dt
dt_st = '2022-11-03 00:00:00'
# dt_st = '2022-09-15 00:00:00'
dt_st_fmt = datetime.strptime(dt_st, '%Y-%m-%d %H:%M:%S')

#end dt
dt_end_fmt = dt_st_fmt + timedelta(hours = (24*60))
dt_end = dt_end_fmt.strftime('%Y-%m-%d %H:%M:%S')
# dt_end = '2022-12-03 23:50:00'

query = ("SELECT * FROM analysis_db.gnss_testa\
          where ts between ('%s') and ('%s')" % (dt_st,dt_end))
# query = ("SELECT * FROM analysis_db.gnss_sinsa\
          # where ts between ('%s') and ('%s')" % (dt_st,dt_end))

sql_data = pd.read_sql(query,connection)
data = pd.DataFrame(sql_data)
data.reset_index()

G_ACCURACY = 0.0141

data_acc = data.loc[(data['accuracy'] == G_ACCURACY)].\
            reset_index(drop=True).\
            sort_values(by='ts', ascending=True, ignore_index=True)

data_comp_deci = data_acc[data_acc['latitude'].astype(str).str[-10].eq('.')\
            & data_acc['longitude'].astype(str).str[-10].eq('.')]

df = data_comp_deci[['ts','fix_type','latitude','longitude','accuracy']].copy()
df['msl_rounded'] = np.round(data_comp_deci.msl,2)

msl_zip = list(df.msl_rounded)
counter_msl_zip = Counter(msl_zip)
df_counter_mslzip = pd.DataFrame.from_dict(counter_msl_zip,\
                                           orient='index').reset_index()
df_counter_mslzip.columns = ['msl_rounded','freq']
df_counter_mslzip = df_counter_mslzip.reset_index(drop=True)
new_df = pd.merge(df, df_counter_mslzip, on='msl_rounded').\
            sort_values(by='ts', ascending=True, ignore_index=True)

freq_filtered_df = new_df.loc[(new_df.freq > \
                      (new_df.freq.max() * .5)) &\
                      (new_df.freq != 1)].\
                      sort_values(by='ts', ascending=True, ignore_index=True)


fig = plt.figure()
# gs = gridspec.GridSpec(2, 1)
gs = gridspec.GridSpec(3, 1)

plt.subplot(gs[0,0])
plt.scatter(new_df.ts, new_df.latitude, marker='.', color='blue')
plt.plot(freq_filtered_df.ts, freq_filtered_df.latitude, color='red') 

plt.subplot(gs[1,0])
plt.scatter(new_df.ts, new_df.longitude, marker='.', color='blue')
plt.plot(freq_filtered_df.ts, freq_filtered_df.longitude, color='red')

plt.subplot(gs[2,0])
plt.scatter(new_df.ts, new_df.msl_rounded, marker='.', color='blue')
plt.plot(freq_filtered_df.ts, freq_filtered_df.msl_rounded, color='red')
 

def outlier_filter(df):
    dff = df.copy()

    dfmean = dff[['latitude','longitude','msl_rounded']].\
            rolling(min_periods=1,window=143,center=False).mean()
    dfsd = dff[['latitude','longitude','msl_rounded']].\
            rolling(min_periods=1,window=143,center=False).std()

    dfulimits = dfmean + (3*dfsd)
    dfllimits = dfmean - (3*dfsd)

    dff.latitude[(dff.latitude > dfulimits.latitude) | \
        (dff.latitude < dfllimits.latitude)] = np.nan
    dff.longitude[(dff.longitude > dfulimits.longitude) | \
        (dff.longitude < dfllimits.longitude)] = np.nan
    dff.msl_rounded[(dff.msl_rounded > dfulimits.msl_rounded) | \
        (dff.msl_rounded < dfllimits.msl_rounded)] = np.nan

    dflogic = dff.latitude * dff.longitude * dff.msl_rounded
    dff = dff[dflogic.notnull()]

    return dff

df_outlier_applied = outlier_filter(freq_filtered_df)

                      
fig = plt.figure()
# gs = gridspec.GridSpec(2, 1)
gs = gridspec.GridSpec(3, 1)

plt.subplot(gs[0,0])
plt.scatter(freq_filtered_df.ts, freq_filtered_df.latitude, marker='.', color='blue')
plt.plot(df_outlier_applied.ts, df_outlier_applied.latitude, marker='.',color='red') 

plt.subplot(gs[1,0])
plt.scatter(freq_filtered_df.ts, freq_filtered_df.longitude, marker='.', color='blue')
plt.plot(df_outlier_applied.ts, df_outlier_applied.longitude, marker='.',color='red')

plt.subplot(gs[2,0])
plt.scatter(freq_filtered_df.ts, freq_filtered_df.msl_rounded, marker='.', color='blue')
plt.plot(df_outlier_applied.ts, df_outlier_applied.msl_rounded, marker='.',color='red')



print("data: ",len(data))
print(" --- unique lat: ", data["latitude"].unique())
print(" --- unique lon: ", data["longitude"].unique())

print("data_acc:",len(data_acc))
print(" --- unique lat: ", data_acc["latitude"].unique())
print(" --- unique lon: ", data_acc["longitude"].unique())

print("data_comp_deci:",len(data_comp_deci))
print(" --- unique lat: ", data_comp_deci["latitude"].unique())
print(" --- unique lon: ", data_comp_deci["longitude"].unique())

print("new_df:",len(new_df))
print(" --- unique lat: ", new_df["latitude"].unique())
print(" --- unique lon: ", new_df["longitude"].unique())
print(" --- unique msl: ", new_df["msl_rounded"].unique())

print("freq_filtered_df:",len(freq_filtered_df))
print(" --- unique lat: ", freq_filtered_df["latitude"].unique())
print(" --- unique lon: ", freq_filtered_df["longitude"].unique())
print(" --- unique msl: ", freq_filtered_df["msl_rounded"].unique())

print("df_outlier_applied:",len(df_outlier_applied))
print(" --- unique lat: ", df_outlier_applied["latitude"].unique())
print(" --- unique lon: ", df_outlier_applied["longitude"].unique())
print(" --- unique msl: ", df_outlier_applied["msl_rounded"].unique())



