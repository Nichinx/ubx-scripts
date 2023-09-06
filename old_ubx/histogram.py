import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import numpy as np 
import math
from textwrap import wrap
# from mpl_toolkits.basemapt import Basemap
from collections import Counter
import geopandas

pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.max_column', None)
# pd.set_option('display.max_colwidth', None)
# pd.set_option('display.max_rows', None)
# pd.options.display.float_format = "{:,.9f}".format

db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="senslope",
    database="new_schema")
query = "SELECT * FROM new_schema.ublox_sinua where ts > '2022-07-27'"
# query = "SELECT * FROM analysis_db.ublox_sinua order by ts desc" #limit 200"


data = pd.read_sql(query,db)
data = data.sort_values(by='ts', ascending=True, ignore_index=True)

data['ts'] = pd.to_datetime(data['ts'])
data['latitude'] = pd.to_numeric(data['latitude'])
data['longitude'] = pd.to_numeric(data['longitude'])
# data['accuracy'] = pd.to_numeric(data['accuracy'])
data['msl'] = pd.to_numeric(data['msl'])
# data['temp'] = pd.to_numeric(data['temp'], errors='coerce').isnull()
data['volt'] = pd.to_numeric(data['volt'])


post_zip = list(zip(data.longitude, data.latitude))
counter_post = Counter(post_zip)
cp_df = pd.DataFrame.from_dict(counter_post, orient='index').reset_index()
cp_df.columns = ['longlat','freq']
cp_df_top = cp_df.loc[(cp_df['freq'] >= 15)]
cp_df_top_ri = cp_df_top.reset_index(drop=True)



query_no_sinua_data = "SELECT * FROM new_schema.sms_no_sinua"
sql_data_query2 = pd.read_sql(query_no_sinua_data,db)
df_no_sin_data = pd.DataFrame(sql_data_query2)
counter_NSD = Counter(df_no_sin_data.sms_msg)
new_cp_df = pd.DataFrame.from_dict(counter_NSD, orient='index').reset_index()
new_cp_df.columns = ['longlat','freq']
concat_cp_df = pd.concat([cp_df_top, new_cp_df], axis=0)


concat_cp_df_ri = concat_cp_df.reset_index(drop=True)




def addlabels(x,y):
    for i in range(len(x)):
        plt.text(i, y[i], y[i], ha='center')

        
x = cp_df_top_ri.longlat
# x_df_no_NSD = concat_cp_df_ri.iloc[[0,1,2,3,4,5,6,7,8,9]]
# x = x_df_no_NSD.longlat
xarr = np.stack(x).astype(None)
y = (cp_df_top_ri.freq/len(data.ts))*100
# y = (concat_cp_df_ri.freq/len(data.ts))*100
y = np.round(y,2)


ax = cp_df_top_ri.plot(kind="bar", width=0.5, align='center')
# ax = concat_cp_df_ri.plot(kind="bar", width=0.5, align='center')
# ax = concat_cp_df_ri.plot(kind="bar", width=0.5, align='center')
ax.set_title("Position data vs Frequency")
ax.set_xticklabels(xarr)
addlabels(x,y)

figManager = plt.get_current_fig_manager()
figManager.window.showMaximized()




y_cperc = (concat_cp_df_ri.freq/len(data.ts))*100
concat_cp_df_perc = pd.concat([concat_cp_df_ri, y_cperc], axis=1)



# fig, ax = plt.subplots()
# plot = ax.bar(cp_df_top_ri.index,cp_df_top_ri.freq)
# ax.set_xticks(cp_df_top_ri.latlong)


# gdf = geopandas.GeoDataFrame(geopandas.points_from_xy(data.latitude, data.longitude))
# geopandas.options.display_precision = 9



###########################################################################################
# pd.crosstab(data.latitude, data.longitude)  #FREQUENCY COUNTER
###########################################################################################
