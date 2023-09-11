import mysql.connector
import pandas as pd
import numpy as np 
import gmplot
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


query = "SELECT * FROM analysis_db.gnss_testa ORDER BY ts desc"

data = pd.read_sql(query,db)

lats = data.latitude
lons = data.longitude


###pivs loc test site - unfiltered
gmap1 = gmplot.GoogleMapPlotter(14.651967, 121.058481, 21.5, maptype="satellite") 
gmap1.scatter(lats, lons, color='#3B0B39', size=.3, marker=False)
gmap1.draw('tesua_sept.html')



#lat lon frequency counter
lat_lon_list_unfiltered = list(zip(data.latitude, data.longitude))
lat_lon_counter_unf = Counter(lat_lon_list_unfiltered)
df_lat_lon_counter_unf = pd.DataFrame.from_dict(lat_lon_counter_unf, 
                                            orient='index').reset_index()
df_lat_lon_counter_unf.columns = ['lat_lon','freq']
df_lat_lon_counter_unf = df_lat_lon_counter_unf.reset_index(drop=True)
lat_long_freq_unf = df_lat_lon_counter_unf.loc[(df_lat_lon_counter_unf.freq != 1)]
print(lat_long_freq_unf)

#histogram
def addlabels(x,y):
    for i in range(len(x)):
        plt.text(i, y[i], y[i], ha='center')
    
x_axis = lat_long_freq_unf.lat_lon
x_axis_array = np.stack(x_axis).astype(None)
y_axis = (lat_long_freq_unf.freq/len(data.ts))*100
y_axis = np.round(y_axis,3)
ax = lat_long_freq_unf.plot(kind="bar", width=0.15, align='center')

ax.set_title("Position coordinate data vs Frequency (unfiltered)")
ax.set_xticklabels(x_axis_array)
# addlabels(x_axis,y_axis)


###############################################################################

#filtered
data_filtered = data.loc[(data['hacc'] == 0.0141) & 
                (data['fix_type'] == 2) & 
                (data['vacc'] == 0.01)].\
            reset_index(drop=True).\
            sort_values(by='ts', ascending=True, ignore_index=True)
data_filtered = data_filtered[data_filtered['latitude'].astype(str).str[-10].eq('.') &
                              data_filtered['longitude'].astype(str).str[-10].eq('.')]


data_filtered_msl = data_filtered[['ts',
                                    'fix_type',
                                    'latitude',
                                    'longitude',
                                    'hacc',
                                    'vacc']].copy()
data_filtered_msl['msl_rounded'] = np.round(data_filtered.msl,2)

msl_zip = list(data_filtered_msl.msl_rounded)
counter_msl_zip = Counter(msl_zip)
df_counter_msl_zip = pd.DataFrame.from_dict(counter_msl_zip,
                                            orient='index').reset_index()
df_counter_msl_zip.columns = ['msl_rounded','freq']
df_counter_msl_zip = df_counter_msl_zip.reset_index(drop=True)
new_df_msl_filtered = pd.merge(data_filtered_msl, df_counter_msl_zip, on='msl_rounded').\
            sort_values(by='ts', ascending=True, ignore_index=True)

msl_freq_filtered_df = new_df_msl_filtered.loc[(new_df_msl_filtered.freq != 1)].\
                      sort_values(by='ts', ascending=True, ignore_index=True)


filtered_lats = msl_freq_filtered_df.latitude
filtered_lons = msl_freq_filtered_df.longitude

###pivs loc test site - filtered
gmap2 = gmplot.GoogleMapPlotter(14.651967, 121.058481, 21.5, maptype="satellite") 
gmap2.scatter(filtered_lats, filtered_lons, color='#3B0B39', size=.3, marker=False)
gmap2.draw('tesua_sept_filt.html')



#lat lon frequency counter
lat_lon_list = list(zip(msl_freq_filtered_df.latitude, msl_freq_filtered_df.longitude))
lat_lon_counter = Counter(lat_lon_list)
df_lat_lon_counter = pd.DataFrame.from_dict(lat_lon_counter, 
                                            orient='index').reset_index()
df_lat_lon_counter.columns = ['lat_lon','freq']
df_lat_lon_counter = df_lat_lon_counter.reset_index(drop=True)
print(df_lat_lon_counter)

#histogram
def addlabels(x,y):
    for i in range(len(x)):
        plt.text(i, y[i], y[i], ha='center')
    
x_axis = df_lat_lon_counter.lat_lon
x_axis_array = np.stack(x_axis).astype(None)
y_axis = (df_lat_lon_counter.freq/len(msl_freq_filtered_df.ts))*100
y_axis = np.round(y_axis,2)
ax = df_lat_lon_counter.plot(kind="bar", width=0.15, align='center')

ax.set_title("Position coordinate data vs Frequency (filtered)")
ax.set_xticklabels(x_axis_array)
addlabels(x_axis,y_axis)
                      