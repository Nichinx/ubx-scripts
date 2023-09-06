import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import numpy as np 
import math
from textwrap import wrap
from collections import Counter
import geopandas

pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.max_column', None)
# pd.set_option('display.max_colwidth', None)
# pd.set_option('display.max_rows', None)
pd.options.display.float_format = "{:,}".format

db = mysql.connector.connect(
    host="192.168.150.112",
    user="pysys_local",
    password="NaCAhztBgYZ3HwTkvHwwGVtJn5sVMFgg",
    database="analysis_db")
# query = "SELECT * FROM analysis_db.ublox_sinua" #old sinua query
# query = "SELECT * FROM analysis_db.gnss_sinsa" #new sinua query
query = "SELECT * FROM analysis_db.gnss_testa" #testa query

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
cp_df_top = cp_df.loc[(cp_df.freq >= (cp_df.freq.max()*.05))] #above 5% of max frequency
# cp_df_top = cp_df.loc[(cp_df.freq >= 10)] #above 2 digits
cp_df_top_ri = cp_df_top.reset_index(drop=True)


# #old sinua query -- initial, before FW
# query_no_sinua_data = "SELECT * FROM comms_db.smsinbox_loggers where sms_msg like 'no sinua%' and mobile_id = 143 and ts_sms < '2022-07-23'" 
# sql_data_query2 = pd.read_sql(query_no_sinua_data,db)
# df_no_sin_data = pd.DataFrame(sql_data_query2)
# counter_NSD = Counter(df_no_sin_data.sms_msg)
# new_cp_df = pd.DataFrame.from_dict(counter_NSD, orient='index').reset_index(drop=True)
# concat_cp_df = pd.concat([cp_df_top, new_cp_df], axis=0)
# total_data_frequency = concat_cp_df.reset_index(drop=True)
# total_data_frequency.columns = ['longlat','freq']

# #new sinua query -- dated after FW
# query_no_sinua_data = "SELECT * FROM comms_db.smsinbox_loggers where sms_msg like 'no sinua%' and mobile_id = 143 and ts_sms > '2022-07-23'"
# sql_data_query2 = pd.read_sql(query_no_sinua_data,db)
# df_no_sin_data = pd.DataFrame(sql_data_query2)
# counter_NSD = Counter(df_no_sin_data.sms_msg)
# new_cp_df = pd.DataFrame.from_dict(counter_NSD, orient='index').reset_index()
# new_cp_df.columns = ['longlat','freq']
# total_data_frequency = pd.concat([cp_df_top, new_cp_df], axis=0)

#testa query
query_no_sinua_data = "SELECT * FROM comms_db.smsinbox_loggers where sms_msg like 'no ublox%' and mobile_id = 168"
sql_data_query2 = pd.read_sql(query_no_sinua_data,db)
df_no_sin_data = pd.DataFrame(sql_data_query2)
counter_NSD = Counter(df_no_sin_data.sms_msg)
new_cp_df = pd.DataFrame.from_dict(counter_NSD, orient='index').reset_index()
new_cp_df.columns = ['longlat','freq']
total_data_frequency = pd.concat([cp_df_top, new_cp_df], axis=0)

    
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
ax.set_title("Position Coordinates Data vs. Frequency")
ax.set_xticklabels(xarr)
addlabels(x,y)

figManager = plt.get_current_fig_manager()
figManager.window.showMaximized()


# #old sinua query
# y_cperc = (total_data_frequency.freq/len(data.ts))*100
# total_data_frequency_with_perc = pd.concat([total_data_frequency, np.round(y_cperc,2)], axis=1)
# total_data_frequency_with_perc = total_data_frequency_with_perc.reset_index(drop=True)
# total_data_frequency_with_perc_and_sign = total_data_frequency_with_perc.iloc[:,2].astype(str).str.cat(['%']*len(total_data_frequency_with_perc))
# total_data_frequency_with_perc_and_sign = pd.concat([total_data_frequency, total_data_frequency_with_perc_and_sign], axis=1)
# total_data_frequency_with_perc_and_sign.columns = ['long_lat_coordinates','frequency','percent_freq']
# # total_data_frequency_with_perc.columns = ['longlat','freq','percent_freq']
# # total_data_frequency_with_perc = total_data_frequency_with_perc.percent_freq.astype(str).str.cat(['%']*len(total_data_frequency_with_perc))

# #new sinua query
# y_cperc = (total_data_frequency.freq/len(data.ts))*100
# total_data_frequency_with_perc = pd.concat([total_data_frequency, np.round(y_cperc,2)], axis=1)
# total_data_frequency_with_perc_and_sign = total_data_frequency_with_perc.iloc[:,2].astype(str).str.cat(['%']*len(total_data_frequency_with_perc))
# total_data_frequency_with_perc_and_sign = pd.concat([total_data_frequency, total_data_frequency_with_perc_and_sign], axis=1)
# total_data_frequency_with_perc_and_sign.columns = ['long_lat_coordinates','frequency','percent_freq']

#testa query
y_cperc = (total_data_frequency.freq/len(data.ts))*100
total_data_frequency_with_perc = pd.concat([total_data_frequency, np.round(y_cperc,2)], axis=1)
total_data_frequency_with_perc_and_sign = total_data_frequency_with_perc.iloc[:,2].astype(str).str.cat(['%']*len(total_data_frequency_with_perc))
total_data_frequency_with_perc_and_sign = pd.concat([total_data_frequency, total_data_frequency_with_perc_and_sign], axis=1)
total_data_frequency_with_perc_and_sign.columns = ['long_lat_coordinates','frequency','percent_freq']








