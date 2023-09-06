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

connection = mysql.connector.connect(
                    host='192.168.150.112',
                    user='pysys_local',
                    password='NaCAhztBgYZ3HwTkvHwwGVtJn5sVMFgg',
                    database='analysis_db')

#start dt
dt_st = '2022-09-01 00:00:00'
dt_st_fmt = datetime.strptime(dt_st, '%Y-%m-%d %H:%M:%S')

#end dt
dt_end_fmt = dt_st_fmt + timedelta(hours = 24*30)
dt_end = dt_end_fmt.strftime('%Y-%m-%d %H:%M:%S')
# dt_end = '2022-12-03 23:50:00'


#old sinua query
# query = ("SELECT * FROM analysis_db.ublox_sinua\
            # where ts between ('%s') and ('%s')" % (dt_st,dt_end)) 
#new sinua query
query = ("SELECT * FROM analysis_db.gnss_sinsa\
            where ts between ('%s') and ('%s')" % (dt_st,dt_end))
#tesua query
# query = ("SELECT * FROM analysis_db.gnss_testa\
            # where ts between ('%s') and ('%s')" % (dt_st,dt_end))

sql_data = pd.read_sql(query,connection)
data = pd.DataFrame(sql_data)
data.reset_index()
G_ACCURACY = 0.0141

data_acc = data.loc[(data['accuracy'] == G_ACCURACY)].\
            reset_index(drop=True).\
            sort_values(by='ts', ascending=True, ignore_index=True)

#9 deci count -- ver1
# data_comp_deci = data_acc[data_acc['latitude'].astype(str).str[-10].eq('.')\
#             & data_acc['longitude'].astype(str).str[-10].eq('.')]

#9 deci count -- ver2    
data_comp_deci = data_acc.loc[data_acc['latitude'].astype(str).\
                    str.match("^\d*\.(\d{9}$)") & data_acc['longitude'].\
                    astype(str).str.match("^\d*\.(\d{9}$)")]


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

# #filter by frequency count : drop freq less than 50% and freq=1
# freq_filtered_df = new_df.loc[(new_df.freq > \
#                       (np.round((new_df.freq.max() * .5),2))) &\
#                       (new_df.freq != 1)].\
#                       sort_values(by='ts', ascending=True, ignore_index=True)
                              
# fig = plt.figure()
# gs = gridspec.GridSpec(2, 1)

# plt.subplot(gs[0,0])
# plt.scatter(new_df.ts, new_df.latitude, marker='.', color='blue')
# plt.plot(freq_filtered_df.ts, freq_filtered_df.latitude, color='red') 

# plt.subplot(gs[1,0])
# plt.scatter(new_df.ts, new_df.longitude, marker='.', color='blue')
# plt.plot(freq_filtered_df.ts, freq_filtered_df.longitude, color='red')

# def outlier_filter(df):
#     dff = df.copy()

#     dfmean = dff[['latitude','longitude','msl_rounded']].\
#             rolling(min_periods=1,window=6,center=False).mean()
#     dfsd = dff[['latitude','longitude','msl_rounded']].\
#             rolling(min_periods=1,window=6,center=False).std()

#     #setting of limits
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

# df_outlier_applied = outlier_filter(freq_filtered_df)

# fig = plt.figure()
# gs = gridspec.GridSpec(2, 1)

# plt.subplot(gs[0,0])
# plt.scatter(freq_filtered_df.ts, freq_filtered_df.latitude, \
#             marker='.', color='blue')
# plt.plot(df_outlier_applied.ts, df_outlier_applied.latitude, \
#             marker='.',color='red') 

# plt.subplot(gs[1,0])
# plt.scatter(freq_filtered_df.ts, freq_filtered_df.longitude, \
#             marker='.', color='blue')
# plt.plot(df_outlier_applied.ts, df_outlier_applied.longitude, \
#             marker='.',color='red')      
    
    
    

##########ROLLING WINDOW
# def windows_applied():
#     dff = new_df.copy()
#     def freq_filt(df):
#                     dff = df.loc[(df.freq > \
#                            (df.freq.max() * .5)) &\
#                            (df.freq != 1)].\
#                            sort_values(by='ts', ascending=True, ignore_index=True)
#     return dff
#     result = dff.rolling(6).apply(freq_filt)
#     return result
# print(windows_applied())


# def freq_filt(x):
#     return x.loc[(x.freq > (x.freq.max() * .5)) & (x.freq != 1)].\
#                 sort_values(by='ts', ascending=True, ignore_index=True)



# re.search("(^\d+\.?(\d){9}$)", data_acc.latitude)
# new_df.loc[new_df['latitude'].astype(str).str.match("^\d+\.?\d{9}$")]