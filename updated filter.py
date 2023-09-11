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
# print('total data = ',len(data))

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

msl_freq_filtered_df = new_df_msl_filtered.loc[(new_df_msl_filtered.freq > \
                      (new_df_msl_filtered.freq.max() * .5)) &\
                      (new_df_msl_filtered.freq != 1)].\
                      sort_values(by='ts', ascending=True, ignore_index=True)
print('msl_freq_filtered_df = ',len(msl_freq_filtered_df))   

#LATLON frequency counter
latlon_zip = list(zip(msl_freq_filtered_df.latitude, 
                      msl_freq_filtered_df.longitude))
counter_latlon_zip = Counter(latlon_zip)
df_counter_latlon_zip = pd.DataFrame.from_dict(counter_latlon_zip, 
                                           orient='index').reset_index()
df_counter_latlon_zip.columns = ['latlon','freq']
df_counter_latlon_zip = df_counter_latlon_zip.reset_index(drop=True)




# ##outlier
# def outlier_filter(df):
#     dff = df.copy()

#     dfmean = dff[['latitude','longitude','msl_rounded']].\
#             rolling(min_periods=1,window=6,center=False).mean()
#     dfsd = dff[['latitude','longitude','msl_rounded']].\
#             rolling(min_periods=1,window=6,center=False).std()

#     dfulimits = dfmean + (1*dfsd)
#     dfllimits = dfmean - (1*dfsd)

#     dff.latitude[(dff.latitude > dfulimits.latitude) | \
#         (dff.latitude < dfllimits.latitude)] = np.nan
#     dff.longitude[(dff.longitude > dfulimits.longitude) | \
#         (dff.longitude < dfllimits.longitude)] = np.nan
#     dff.msl_rounded[(dff.msl_rounded > dfulimits.msl_rounded) | \
#         (dff.msl_rounded < dfllimits.msl_rounded)] = np.nan

#     dflogic = dff.latitude * dff.longitude * dff.msl_rounded
#     dff = dff[dflogic.notnull()]

#     return dff

# df_outlier_applied = outlier_filter(df)
# print(df_outlier_applied)


##mode
# s = df.squeeze()
# for window in s.rolling(window=6):
#     latlon_zip = list(zip(df.latitude, 
#                           df.longitude))
#     counter_latlon_zip = Counter(latlon_zip).most_common(1)
#     df_counter_latlon_zip = pd.DataFrame.from_dict(counter_latlon_zip, 
#                                                 orient='index').reset_index()
#     df_counter_latlon_zip.columns = ['latlon','freq']
#     df_counter_latlon_zip = df_counter_latlon_zip.reset_index(drop=True)

# print(df_counter_latlon_zip)

#######
# latlon_zip = list(zip(df.latitude,df.longitude))
# counter_latlon_zip = Counter(latlon_zip)
# latlon_mode = pd.DataFrame.from_records(counter_latlon_zip.most_common(1), columns=['latlon','freq']).reset_index(drop=True)
#######









