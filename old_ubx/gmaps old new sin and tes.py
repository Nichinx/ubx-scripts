import mysql.connector
import pandas as pd
import numpy as np 
import math
import gmplot

pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.max_column', None)
# pd.set_option('display.max_rows', None)
# pd.options.display.float_format = "{:,.9f}".format

#raw and filtered plot

db = mysql.connector.connect(
    host="192.168.150.112",
    user="pysys_local",
    password="NaCAhztBgYZ3HwTkvHwwGVtJn5sVMFgg",
    database="analysis_db")

# query = "SELECT * FROM analysis_db.ublox_sinua  ORDER BY ts desc" #old sinua query
# query = "SELECT * FROM analysis_db.gnss_sinsa ORDER BY ts desc" #new sinua query
query = "SELECT * FROM analysis_db.gnss_testa  ORDER BY ts desc" #testa query


data = pd.read_sql(query,db)
# data = data.sort_values(by='ts', ascending=True, ignore_index=True)
# data_filtered = data.loc[(data['prec'] == 0.0141)].reset_index(drop=True).sort_values(by='ts', ascending=True, ignore_index=True) #old sinua query
# data_filtered = data.loc[(data['accuracy'] == 0.0141)].reset_index(drop=True).sort_values(by='ts', ascending=True, ignore_index=True)    
data_filtered = data.loc[(data['accuracy'] == 0.0141) & (data['fix_type'] == 2)].reset_index(drop=True).sort_values(by='ts', ascending=True, ignore_index=True) #new sinua query | tesua query


lats = data.latitude
lons = data.longitude
lats_filtered = data_filtered.latitude
lons_filtered = data_filtered.longitude


###sin loc
# gmap = gmplot.GoogleMapPlotter(16.723503113, 120.781272888, 22, maptype="hybrid")
# gmap = gmplot.GoogleMapPlotter(16.723461151, 120.781311035, 22, maptype="hybrid")

# ###pivs loc test site
# gmap = gmplot.GoogleMapPlotter(14.651967, 121.058481, 22, maptype="hybrid") 
gmap = gmplot.GoogleMapPlotter(14.651926994, 121.058448792, 22, maptype="hybrid") 


# # sin_bb = zip(*[
# #     (16.724067, 120.780691),
# #     (16.724089, 120.781932),
# #     (16.722875, 120.781981),
# #     (16.722868, 120.780710),
# # ])
# # gmap.polygon(*sin_bb, color='cornflowerblue', edge_width=10)
# gmap.marker(16.723444, 120.781350, color='cornflowerblue')        #SINUA MARKER - OLD 
#### gmap.marker(16.723503113, 120.781272888, color='cornflowerblue')    #SINUA MARKER - based on deployment pptx
# gmap.marker(16.723461151, 120.781311035, color='cornflowerblue')
# gmap.marker(14.651927, 121.058433, color='cornflowerblue')      #TESUA MARKER
gmap.marker(14.651926994, 121.058448792, color='cornflowerblue')


# gmap.scatter(lats, lons, color='#3B0B39', size=.3, marker=False)                      #raw lats&lon
gmap.scatter(lats_filtered, lons_filtered, color='#3B0B39', size=.3, marker=False)    #filtered lats&lon


# gmap.draw('old_sin_raw.html')         #1
# gmap.draw('old_sin_filtered.html')    #2
# gmap.draw('new_sin_raw.html')         #3
# gmap.draw('new_sin_filtered.html')    #4
# gmap.draw('tesua_raw.html')           #5
gmap.draw('tesua_filtered.html')      #6