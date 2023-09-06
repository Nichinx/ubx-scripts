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

pd.set_option('display.expand_frame_repr', None)
pd.set_option('display.max_column', None)
# pd.set_option('display.max_rows', None)
pd.options.display.float_format = "{:,}".format

G_ACCURACY = 0.0141

try:
    connection = mysql.connector.connect(
                        host='192.168.150.112',
                        user='pysys_local',
                        password='NaCAhztBgYZ3HwTkvHwwGVtJn5sVMFgg',
                        database='analysis_db')
    
    # connection = mysql.connector.connect(
    #                     host="localhost",
    #                     user="root",
    #                     password="senslope",
    #                     database="new_schema")
    
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
dt_st = '2022-11-03 00:00:00' #start ng tesua na tuloy tuloy na
# dt_st = '2022-07-26 12:00:00'
# dt_st = '2022-09-24 00:00:00'
dt_st_fmt = datetime.strptime(dt_st, '%Y-%m-%d %H:%M:%S')

#end dt
dt_end_fmt = dt_st_fmt + timedelta(hours = 48)
dt_end = dt_end_fmt.strftime('%Y-%m-%d %H:%M:%S')

#sql query
query = ("SELECT * FROM analysis_db.gnss_testa\
          where ts between ('%s') and ('%s')" 
          % (dt_st,dt_end))

# query = ("SELECT * FROM analysis_db.gnss_sinsa\
#          where ts between ('%s') and ('%s')" 
#          % (dt_st,dt_end))
    
# query = ("SELECT * FROM new_schema.ublox_sinua\
#          where ts between ('%s') and ('%s')" 
#          % (dt_st,dt_end))
    
sql_data = pd.read_sql(query,connection)
data = pd.DataFrame(sql_data)
data.reset_index()

#initial filter for rtk fix, accuracy=0.0141
data = data.loc[(data['accuracy'] == G_ACCURACY)].\
            reset_index(drop=True).\
            sort_values(by='ts', ascending=True, ignore_index=True) # sa query pa lang pwede na isort para di na magsort dito. --- (hindi sya nagw-work before sa query ko kaya naging ganto fix ko, wala syang nirereturn sakin thoooo before ay sa query palang nga nagffilter na ako)

#added filter, locate lat & long with 9 deci, drop if less
data = data[data['latitude'].astype(str).str[-10].eq('.')\
            & data['longitude'].astype(str).str[-10].eq('.')] 
    
#for funt: data.loc[data.iloc[:,3].astype(str).str[-10].eq('.') & data.iloc[:,4].astype(str).str[-10].eq('.')]
# def drop_deci(df):
#     df_deci_dropped = df.copy()
#     df_deci_dropped = df_deci_dropped.loc[df_deci_dropped.iloc[:,3].\
#                             astype(str).str[-10].eq('.') & \
#                             df_deci_dropped.iloc[:,4].\
#                             astype(str).str[-10].eq('.')]
#     return df_deci_dropped
 
# data['ts'] = pd.to_datetime(data['ts'])
# data['latitude'] = pd.to_numeric(data['latitude'])
# data['longitude'] = pd.to_numeric(data['longitude'])
# data['accuracy'] = pd.to_numeric(data['accuracy'])
# data['msl'] = pd.to_numeric(data['msl'])
# data['volt'] = pd.to_numeric(data['volt'])

df = data[['ts','fix_type','latitude','longitude','accuracy']].copy()
df['msl_rounded'] = np.round(data.msl,2) # oohhhh interesting. pwedeng ivary yung rounding up or down

msl_zip = list(df.msl_rounded)
counter_msl_zip = Counter(msl_zip)
df_counter_mslzip = pd.DataFrame.from_dict(counter_msl_zip,\
                                           orient='index').reset_index()
df_counter_mslzip.columns = ['msl_rounded','freq']
df_counter_mslzip = df_counter_mslzip.reset_index(drop=True)
new_df = pd.merge(df, df_counter_mslzip, on='msl_rounded').\
            sort_values(by='ts', ascending=True, ignore_index=True)
            
#filter by frequency count : drop freq less than 50% and freq=1
freq_filtered_df = new_df.loc[(new_df.freq > \
                      (np.round((new_df.freq.max() * .5),2))) &\
                      (new_df.freq != 1)].\
                      sort_values(by='ts', ascending=True, ignore_index=True)
                      
                  
# #initial plot
# fig = plt.figure()
# gs = gridspec.GridSpec(2, 1)

# plt.subplot(gs[0,0])
# plt.scatter(new_df.ts, new_df.latitude, marker='.', color='blue')
# plt.plot(freq_filtered_df.ts, freq_filtered_df.latitude, color='red') 

# plt.subplot(gs[1,0])
# plt.scatter(new_df.ts, new_df.longitude, marker='.', color='blue')
# plt.plot(freq_filtered_df.ts, freq_filtered_df.longitude, color='red') 
                   


###############################################################################
#plot displacements
radius_earth = 6371e3

new_df_copy_disp = new_df[['ts']].copy()
new_df_copy_disp['lat2'] = new_df.latitude
new_df_copy_disp['lon2'] = new_df.longitude
new_df_copy_disp['lat1'] = new_df_copy_disp.lat2.shift(-1)
new_df_copy_disp['lon1'] = new_df_copy_disp.lon2.shift(-1)

phi1_new_df_copy_disp = new_df_copy_disp.lat1 * math.pi/180
phi2_new_df_copy_disp = new_df_copy_disp.lat2 * math.pi/180

new_df_copy_disp['delta_lat'] = (new_df_copy_disp.lat2 - new_df_copy_disp.lat1) * math.pi/180
new_df_copy_disp['delta_lon'] = (new_df_copy_disp.lon2 - new_df_copy_disp.lon1) * math.pi/180
new_df_copy_disp= new_df_copy_disp.fillna(0)

#haversine
haver = np.sin(new_df_copy_disp.delta_lat/2)**2 + np.cos(new_df_copy_disp.lat1) * np.cos(new_df_copy_disp.lat2) * np.sin(new_df_copy_disp.delta_lon/2)**2
sqhvr = haver**(1/2)
sqhvrmin1 = (1-haver)**(1/2)
circ = 2 * np.arctan2(sqhvr, sqhvrmin1)
dist = radius_earth * circ     #in meters
new_df_copy_disp['dist'] = dist

###############################################################################
freq_fil_copy_disp = freq_filtered_df[['ts']].copy()
freq_fil_copy_disp['lat2'] = freq_filtered_df.latitude
freq_fil_copy_disp['lon2'] = freq_filtered_df.longitude
freq_fil_copy_disp['lat1'] = freq_fil_copy_disp.lat2.shift(-1)
freq_fil_copy_disp['lon1'] = freq_fil_copy_disp.lon2.shift(-1)

phi1_freq_fil_copy_disp = freq_fil_copy_disp.lat1 * math.pi/180
phi2_freq_fil_copy_disp = freq_fil_copy_disp.lat2 * math.pi/180

freq_fil_copy_disp['delta_lat'] = (freq_fil_copy_disp.lat2 - freq_fil_copy_disp.lat1) * math.pi/180
freq_fil_copy_disp['delta_lon'] = (freq_fil_copy_disp.lon2 - freq_fil_copy_disp.lon1) * math.pi/180
freq_fil_copy_disp= freq_fil_copy_disp.fillna(0)

#haversine
haver = np.sin(freq_fil_copy_disp.delta_lat/2)**2 + np.cos(freq_fil_copy_disp.lat1) * np.cos(freq_fil_copy_disp.lat2) * np.sin(freq_fil_copy_disp.delta_lon/2)**2
sqhvr = haver**(1/2)
sqhvrmin1 = (1-haver)**(1/2)
circ = 2 * np.arctan2(sqhvr, sqhvrmin1)
dist = radius_earth * circ     #in meters
freq_fil_copy_disp['dist'] = dist
###############################################################################


###25% on frequency
# msl_tol_max = np.round((cp_df_top_ri.freq.max() * 1.05),2)
# msl_tol_min = np.round((cp_df_top_ri.freq.max() * 0.95),2)

###0.5% tolerance on MSL
# msl_tol_max = np.round((cp_df_top_ri.msl_round.max() * 1.005),2)
# msl_tol_min = np.round((cp_df_top_ri.msl_round.max() * 0.995),2)

###plus minus on MSL
# msl_tol_max = np.round((cp_df_top_ri.msl_round.max() + 1),2)
# msl_tol_min = np.round((cp_df_top_ri.msl_round.max() - 1),2)

# new_df_on_tol = df.loc[(df['msl_round'] > msl_tol_min) & (df['msl_round'] < msl_tol_max)]
# # cp_df_top_ri.freq.loc[(cp_df_top_ri.freq >= 1)].sum()
# cp_df_top_ri.loc[(cp_df_top_ri.freq >= (np.round((cp_df_top_ri.freq.max() * .25),2)))]

# new_df_on_tol = df.loc[(df['msl_round'] > msl_tol_min) & (df['msl_round'] < msl_tol_max)]





#outlier filter
# def outlier_filter(df):
#     dff = df.copy()
    
#     dfmean = dff[['latitude','longitude','msl_rounded']].rolling(min_periods=1,window=6,center=False).mean()
#     dfsd = dff[['latitude','longitude','msl_rounded']].rolling(min_periods=1,window=6,center=False).std()

#     dfulimits = dfmean + (0.25*dfsd)
#     dfllimits = dfmean - (0.25*dfsd)

#     dff.msl_rounded[(dff.msl_rounded > dfulimits.msl_rounded) | (dff.msl_rounded < dfllimits.msl_rounded)] = np.nan
#     dflogic = dff.msl_rounded
#     dff = dff[dflogic.notnull()]
   
#     return dff
#################################################################################
def outlier_filter(df):
    dff = df.copy()

    dfmean = dff[['latitude','longitude','msl_rounded']].\
            rolling(min_periods=1,window=6,center=False).mean()
    dfsd = dff[['latitude','longitude','msl_rounded']].\
            rolling(min_periods=1,window=6,center=False).std()
    
    #setting of limits
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


df_outlier_applied = outlier_filter(freq_filtered_df)

# fig = plt.figure()
# gs = gridspec.GridSpec(2, 1)

# plt.subplot(gs[0,0])
# plt.scatter(new_df.ts, new_df.latitude, marker='.', color='blue')
# plt.plot(df_outlier_applied.ts, df_outlier_applied.latitude, marker='.', color='red') 

# plt.subplot(gs[1,0])
# plt.scatter(new_df.ts, new_df.longitude, marker='.', color='blue')
# plt.plot(df_outlier_applied.ts, df_outlier_applied.longitude, marker='.', color='red') 




