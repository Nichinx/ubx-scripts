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
from mysql.connector import Error
import mysql.connector

import warnings
warnings.filterwarnings("ignore")


#CONNECT TO DB
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
        

#QUERY
query = "select * from analysis_db.gnss_testa \
        where ts >= '2023-11-21' \
        order by ts desc"

sql_data = pd.read_sql(query,connection)
data = pd.DataFrame(sql_data)
data.reset_index()
print('data: ', len(data))

new_df = data
new_df['msl'] = np.round(new_df.msl,2)
new_df = new_df.loc[(new_df.fix_type == 2) & (new_df.sat_num > 28)]
print('new_df fix=2, satnum>28: ', len(new_df))


horizontal_accuracy = 0.0141
vertical_accuracy = 0.01205

new_df = new_df.loc[(new_df['hacc'] == horizontal_accuracy) & 
                          (new_df['vacc'] <= vertical_accuracy)].\
                  reset_index(drop=True).\
                  sort_values(by='ts', ascending=True, ignore_index=True)
print('new_df hacc vacc: ', len(new_df))           

new_df = new_df[new_df['latitude'].astype(str).str[-10].eq('.')\
              & new_df['longitude'].astype(str).str[-10].eq('.')]
print('new_df complete deci: ', len(new_df))


df2 = new_df.loc[~(np.isnan(new_df.latitude))].reset_index(drop=True)

df = utm.from_latlon(new_df.latitude.to_numpy(), new_df.longitude.to_numpy())
df = pd.DataFrame([list(df)]).transpose()
df2['lat_UTM'] = df.loc[0].explode(list()).reset_index(drop=True)
df2['lon_UTM'] = df.loc[1].explode(list()).reset_index(drop=True)
print('df2 = ',len(df2))

# #outlier
# def outlier_filter(df):
#     dff = df.copy()

#     dfmean = dff[['latitude','longitude','msl']].\
#             rolling(min_periods=1,window=6,center=False).mean()
#     dfsd = dff[['latitude','longitude','msl']].\
#             rolling(min_periods=1,window=6,center=False).std()

#     dfulimits = dfmean + (1*dfsd)
#     dfllimits = dfmean - (1*dfsd)

#     dff.latitude[(dff.latitude > dfulimits.latitude) | \
#         (dff.latitude < dfllimits.latitude)] = np.nan
#     dff.longitude[(dff.longitude > dfulimits.longitude) | \
#         (dff.longitude < dfllimits.longitude)] = np.nan
#     dff.msl[(dff.msl > dfulimits.msl) | \
#         (dff.msl < dfllimits.msl)] = np.nan

#     dflogic = dff.latitude * dff.longitude * dff.msl
#     dff = dff[dflogic.notnull()]

#     return dff

# df2 = outlier_filter(df2)
# print('df with outlier = ',len(df2))


#LAT LON DISP PLOTTER
#df2
df2['ts'] = pd.to_datetime(df2['ts'], format='%y%m%d%H%M%S')
df2['latitude'] = pd.to_numeric(df2['latitude'])
df2['longitude'] = pd.to_numeric(df2['longitude'])
df2['hacc'] = pd.to_numeric(df2['hacc'])
df2['vacc'] = pd.to_numeric(df2['vacc'])
df2['msl'] = pd.to_numeric(df2['msl'])
# df2['lat_UTM'] = pd.to_numeric(df2['lat_UTM'])
# df2['lon_UTM'] = pd.to_numeric(df2['lon_UTM'])

df2_copy = df2[['ts']].copy()
df2_copy['lat2'] = df2.latitude
df2_copy['lon2'] = df2.longitude
# df2_copy['lat1'] = df2_copy.lat2.shift(-1)
# df2_copy['lon1'] = df2_copy.lon2.shift(-1)
df2_copy['lat1'] = 14.6519573
df2_copy['lon1'] = 121.0584458

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

df2['dist'] = df2_copy.dist


###################
#outlier
def outlier_filter_1(df):
    dff = df.copy()

    dfmean = dff[['latitude','longitude']].\
            rolling(min_periods=1,window=12,center=False).mean()
    dfsd = dff[['latitude','longitude']].\
            rolling(min_periods=1,window=12,center=False).std()

    dfulimits = dfmean + (2*dfsd)
    dfllimits = dfmean - (2*dfsd)

    dff.latitude[(dff.latitude > dfulimits.latitude) | \
        (dff.latitude < dfllimits.latitude)] = np.nan
    dff.longitude[(dff.longitude > dfulimits.longitude) | \
        (dff.longitude < dfllimits.longitude)] = np.nan
    
    dflogic = dff.latitude * dff.longitude
    dff = dff[dflogic.notnull()]

    return dff


def outlier_filter_2(df):
    dff = df.copy()

    dfmean = dff[['msl']].\
            rolling(min_periods=1,window=24,center=False).mean()
    dfsd = dff[['msl']].\
            rolling(min_periods=1,window=24,center=False).std()

    dfulimits = dfmean + (2*dfsd)
    dfllimits = dfmean - (2*dfsd)

    dff.msl[(dff.msl > dfulimits.msl) | \
        (dff.msl < dfllimits.msl)] = np.nan
    

    dflogic = dff.msl
    dff = dff[dflogic.notnull()]

    return dff


def outlier_filter_3(df):
    dff = df.copy()

    dfmean = dff[['dist']].\
            rolling(min_periods=1,window=48,center=False).mean()
    dfsd = dff[['dist']].\
            rolling(min_periods=1,window=48,center=False).std()

    dfulimits = dfmean + (2*dfsd)
    dfllimits = dfmean - (2*dfsd)

    dff.dist[(dff.dist > dfulimits.dist) | \
        (dff.dist < dfllimits.dist)] = np.nan

    dflogic = dff.dist
    dff = dff[dflogic.notnull()]

    return dff


df2 = outlier_filter_1(df2).reset_index(drop=True)
print('df with outlier 1 = ',len(df2))

df2 = outlier_filter_2(df2).reset_index(drop=True)
print('df with outlier 2 = ',len(df2))

df2 = outlier_filter_3(df2).reset_index(drop=True)
print('df with outlier 3 = ',len(df2))
###################




# #moving average
# window_size = 6

# wind_lat2 = df2_copy.lat2.rolling(window_size)
# mva_lat2 = wind_lat2.mean()
# mva_lat2_list = mva_lat2.tolist()
# mva_lat2_final_list = mva_lat2_list[window_size - 1:]

# wind_lon2 = df2_copy.lon2.rolling(window_size)
# mva_lon2 = wind_lon2.mean()
# mva_lon2_list = mva_lon2.tolist()
# mva_lon2_final_list = mva_lon2_list[window_size - 1:]

# wind_msl= df2.msl.rolling(window_size)
# mva_msl = wind_msl.mean()
# mva_msl_list = mva_msl.tolist()
# mva_msl_final_list = mva_msl_list[window_size - 1:]

# wind_dist= df2_copy.dist.rolling(window_size)
# mva_dist = wind_dist.mean()
# mva_dist_list = mva_dist.tolist()
# mva_dist_final_list = mva_dist_list[window_size - 1:]

# df_new_mva  = df2_copy[['ts']].copy()
# df_new_mva.drop(df_new_mva.head((window_size - 1)).index,inplace=True)
# df_new_mva['mva_lat2'] = mva_lat2_final_list
# df_new_mva['mva_lon2'] = mva_lon2_final_list
# df_new_mva['mva_msl'] = mva_msl_final_list
# df_new_mva['mva_dist'] = mva_dist_final_list


#PLOT
fig = plt.figure()
fig.suptitle('TESUA DATA : with outlier filter', fontweight='bold')
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
# plt.plot(df_new_mva.ts,df_new_mva.mva_msl, "red")
plt.ylabel('msl, m', fontweight='bold')
plt.grid(color = 'gray', linestyle = '--', linewidth = 0.5)

plt.subplot(gs[3,0])
# plt.plot(df2_copy.ts, df2_copy.dist, "orange", alpha=0.5)
plt.plot(df2.ts, df2.dist, "orange")
# plt.plot(df_new_mva.ts,df_new_mva.mva_dist, "red")
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
# plt.ylabel('latitude, m', fontweight='bold')
# plt.grid(color = 'gray', linestyle = '--', linewidth = 0.5)
# plt.xticks(rotation=90)
# plt.gca().yaxis.set_major_formatter(FormatStrFormatter('%.6f'))

# plt.subplot(gs[1,0])
# plt.plot(df2.ts, df2.lon_UTM, "blue")
# plt.ylabel('longitude, m', fontweight='bold')
# plt.grid(color = 'gray', linestyle = '--', linewidth = 0.5)
# plt.gca().yaxis.set_major_formatter(FormatStrFormatter('%.6f'))
# plt.xticks(rotation=90)

# plt.show()