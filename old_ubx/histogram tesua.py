#HISTOGRAM
import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np 
from collections import Counter

pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.max_column', None)
pd.set_option('display.max_colwidth', None)
# pd.set_option('display.max_rows', None)
pd.options.display.float_format = "{:,}".format

db = mysql.connector.connect(
    host="192.168.150.112",
    user="pysys_local",
    password="NaCAhztBgYZ3HwTkvHwwGVtJn5sVMFgg",
    database="analysis_db")
query = "SELECT * FROM analysis_db.gnss_testa ORDER BY ts desc"

data = pd.read_sql(query,db)
data = data.loc[(data['accuracy'] == 0.0141)].reset_index(drop=True).sort_values(by='ts', ascending=True, ignore_index=True)

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
cp_df_top_ri = cp_df.reset_index(drop=True)


# df = data[['ts','fix_type','latitude','longitude','accuracy','msl']].copy()
# df['msl_round'] = np.round(data.msl,2)

# post_zip = list(df.msl_round)
# counter_post = Counter(post_zip)
# cp_df = pd.DataFrame.from_dict(counter_post, orient='index').reset_index()
# cp_df.columns = ['msl','freq']
# cp_df_top_ri = cp_df.reset_index(drop=True)


def addlabels(x,y):
    for i in range(len(x)):
        plt.text(i, y[i], y[i], ha='center')
    
x = cp_df_top_ri.longlat
xarr = np.stack(x).astype(None)
# y = (cp_df_top_ri.freq/len(data.ts))*100
# y = np.round(y,2)
ax = cp_df_top_ri.plot(kind="bar", width=0.15, align='center')

ax.set_title("Position data vs Frequency")
ax.set_xticklabels(xarr)
# addlabels(x,y)

figManager = plt.get_current_fig_manager()
figManager.window.showMaximized()

