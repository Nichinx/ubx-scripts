import mysql.connector
import pandas as pd
import numpy as np 
import gmplot
import math
from collections import Counter
import matplotlib.pyplot as plt


pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.max_column', None)
pd.options.display.float_format = "{:,}".format


db = mysql.connector.connect(
    host="192.168.150.112",
    user="pysys_local",
    password="NaCAhztBgYZ3HwTkvHwwGVtJn5sVMFgg",
    database="analysis_db")


query = "select * from analysis_db.gnss_testa \
        where ts >= '2023-11-21' \
        order by ts desc"

data = pd.read_sql(query,db)

lats = data.latitude
lons = data.longitude


###pivs loc test site - unfiltered
apikey = 'AIzaSyA8HsmZuIQ_eQjYxxfuM_J79g6cPMhqXtQ'
gmap1 = gmplot.GoogleMapPlotter(14.651967, 121.058481, 21.5, apikey=apikey, maptype="hybrid") 
gmap1.scatter(lats, lons, color='#3B0B39', size=.3, marker=False)
gmap1.draw('tesua_dec.html')



# #lat lon frequency counter
# lat_lon_list_unfiltered = list(zip(data.latitude, data.longitude))
# lat_lon_counter_unf = Counter(lat_lon_list_unfiltered)
# df_lat_lon_counter_unf = pd.DataFrame.from_dict(lat_lon_counter_unf, 
#                                             orient='index').reset_index()
# df_lat_lon_counter_unf.columns = ['lat_lon','freq']
# df_lat_lon_counter_unf = df_lat_lon_counter_unf.reset_index(drop=True)
# lat_long_freq_unf = df_lat_lon_counter_unf.loc[(df_lat_lon_counter_unf.freq != 1)]
# print(lat_long_freq_unf)

# #histogram
# def addlabels(x,y):
#     for i in range(len(x)):
#         plt.text(i, y[i], y[i], ha='center')
    
# x_axis = lat_long_freq_unf.lat_lon
# x_axis_array = np.stack(x_axis).astype(None)
# y_axis = (lat_long_freq_unf.freq/len(data.ts))*100
# y_axis = np.round(y_axis,3)
# ax = lat_long_freq_unf.plot(kind="bar", width=0.15, align='center')

# ax.set_title("Position coordinate data vs Frequency (unfiltered)")
# ax.set_xticklabels(x_axis_array)
# # addlabels(x_axis,y_axis)


###############################################################################

#filtered
# data_filtered = data.loc[(data['hacc'] == 0.0141) & 
#                 (data['fix_type'] == 2) & 
#                 (data['vacc'] == 0.01)].\
#             reset_index(drop=True).\
#             sort_values(by='ts', ascending=True, ignore_index=True)
# data_filtered = data_filtered[data_filtered['latitude'].astype(str).str[-10].eq('.') &
#                               data_filtered['longitude'].astype(str).str[-10].eq('.')]


# data_filtered_msl = data_filtered[['ts',
#                                     'fix_type',
#                                     'latitude',
#                                     'longitude',
#                                     'hacc',
#                                     'vacc']].copy()
# data_filtered_msl['msl_rounded'] = np.round(data_filtered.msl,2)

# msl_zip = list(data_filtered_msl.msl_rounded)
# counter_msl_zip = Counter(msl_zip)
# df_counter_msl_zip = pd.DataFrame.from_dict(counter_msl_zip,
#                                             orient='index').reset_index()
# df_counter_msl_zip.columns = ['msl_rounded','freq']
# df_counter_msl_zip = df_counter_msl_zip.reset_index(drop=True)
# new_df_msl_filtered = pd.merge(data_filtered_msl, df_counter_msl_zip, on='msl_rounded').\
#             sort_values(by='ts', ascending=True, ignore_index=True)

# msl_freq_filtered_df = new_df_msl_filtered.loc[(new_df_msl_filtered.freq != 1)].\
#                       sort_values(by='ts', ascending=True, ignore_index=True)


# filtered_lats = msl_freq_filtered_df.latitude
# filtered_lons = msl_freq_filtered_df.longitude

# ###pivs loc test site - filtered
# gmap2 = gmplot.GoogleMapPlotter(14.651967, 121.058481, 21.5, apikey=apikey, maptype="hybrid") 
# gmap2.scatter(filtered_lats, filtered_lons, color='#3B0B39', size=.3, marker=False)
# gmap2.draw('tesua_dec_filt.html')



# #lat lon frequency counter
# lat_lon_list = list(zip(msl_freq_filtered_df.latitude, msl_freq_filtered_df.longitude))
# lat_lon_counter = Counter(lat_lon_list)
# df_lat_lon_counter = pd.DataFrame.from_dict(lat_lon_counter, 
#                                             orient='index').reset_index()
# df_lat_lon_counter.columns = ['lat_lon','freq']
# df_lat_lon_counter = df_lat_lon_counter.reset_index(drop=True)
# print(df_lat_lon_counter)

# #histogram
# def addlabels(x,y):
#     for i in range(len(x)):
#         plt.text(i, y[i], y[i], ha='center')
    
# x_axis = df_lat_lon_counter.lat_lon
# x_axis_array = np.stack(x_axis).astype(None)
# y_axis = (df_lat_lon_counter.freq/len(msl_freq_filtered_df.ts))*100
# y_axis = np.round(y_axis,2)
# ax = df_lat_lon_counter.plot(kind="bar", width=0.15, align='center')

# ax.set_title("Position coordinate data vs Frequency (filtered)")
# ax.set_xticklabels(x_axis_array)
# addlabels(x_axis,y_axis)
                      



#####filtered : 12/13

new_df = data
new_df['msl'] = np.round(new_df.msl,2)
new_df = new_df.loc[(new_df.fix_type == 2) & (new_df.sat_num > 28)]


horizontal_accuracy = 0.0141
vertical_accuracy = 0.01205

new_df = new_df.loc[(new_df['hacc'] == horizontal_accuracy) & 
                          (new_df['vacc'] <= vertical_accuracy)].\
                  reset_index(drop=True).\
                  sort_values(by='ts', ascending=True, ignore_index=True)       

new_df = new_df[new_df['latitude'].astype(str).str[-10].eq('.')\
              & new_df['longitude'].astype(str).str[-10].eq('.')]
new_df = new_df.loc[~(np.isnan(new_df.latitude))].reset_index(drop=True)


filtered_lats = new_df.latitude
filtered_lons = new_df.longitude

###pivs loc test site - filtered
gmap2 = gmplot.GoogleMapPlotter(14.651967, 121.058481, 21.5, apikey=apikey, maptype="hybrid") 
gmap2.scatter(filtered_lats, filtered_lons, color='#3B0B39', size=.3, marker=False)
gmap2.draw('tesua_dec_initfilt.html')


df2 = new_df
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
df2 = outlier_filter_2(df2).reset_index(drop=True)
df2 = outlier_filter_3(df2).reset_index(drop=True)


out_filtered_lats = df2.latitude
out_filtered_lons = df2.longitude

###pivs loc test site - filtered
gmap2 = gmplot.GoogleMapPlotter(14.651967, 121.058481, 21.5, apikey=apikey, maptype="hybrid") 
gmap2.scatter(out_filtered_lats, out_filtered_lons, color='#3B0B39', size=.3, marker=False)
gmap2.draw('tesua_dec_outfilt.html')