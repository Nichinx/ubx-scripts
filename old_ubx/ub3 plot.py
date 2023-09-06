import mysql.connector
import pandas as pd
import numpy as np 
import math
import gmplot


pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.max_column', None)
pd.set_option('display.max_rows', None)
# pd.options.display.float_format = "{:,.9f}".format


db = mysql.connector.connect(
    host="192.168.150.112",
    user="pysys_local",
    password="NaCAhztBgYZ3HwTkvHwwGVtJn5sVMFgg",
    database="analysis_db")

# db = mysql.connector.connect(
#     host="localhost",
#     user="root",
#     password="senslope",
#     database="analysis_db")
# query = "SELECT * FROM new_schema.ublox_sinua order by ts desc"
# query = "SELECT * FROM analysis_db.ublox_sinua order by ts desc" #limit 300"

# query = "SELECT * FROM new_db.ubx_tesua \
#         UNION ALL \
#         SELECT ts, logger_id, fix_type, latitude, longitude, accuracy, msl, NULL, volt \
#         FROM new_db.ubx_tesua_notemp \
#         ORDER BY ts"
        
# selecting 1.41cm acc only
# query = "SELECT * FROM new_db.ubx_tesua \
#         WHERE accuracy=0.0141 \
#         UNION ALL \
#         SELECT ts, logger_id, fix_type, latitude, longitude, accuracy, msl, NULL, volt \
#         FROM new_db.ubx_tesua_notemp \
#         WHERE accuracy=0.0141 \
#         ORDER BY ts"

query = "SELECT * FROM analysis_db.gnss_testa ORDER BY ts desc"

data = pd.read_sql(query,db)
# data = data.sort_values(by='ts', ascending=True, ignore_index=True)
data = data.loc[(data['accuracy'] == 0.0141)].reset_index(drop=True).sort_values(by='ts', ascending=True, ignore_index=True)
# data = data.loc[(data['accuracy'] == 0.0141) & (data['fix_type'] == 2)].reset_index(drop=True).sort_values(by='ts', ascending=True, ignore_index=True)

lats = data.latitude
lons = data.longitude


###sin loc
# gmap = gmplot.GoogleMapPlotter(16.723503113, 120.781272888, 21, maptype="hybrid")

###pivs loc test site
gmap = gmplot.GoogleMapPlotter(14.651967, 121.058481, 21, maptype="hybrid") 


# sin_bb = zip(*[
#     (16.724067, 120.780691),
#     (16.724089, 120.781932),
#     (16.722875, 120.781981),
#     (16.722868, 120.780710),
# ])
# gmap.polygon(*sin_bb, color='cornflowerblue', edge_width=10)

# gmap.marker(16.723444, 120.781350, color='cornflowerblue')
gmap.scatter(lats, lons, color='#3B0B39', size=.3, marker=False)


gmap.draw('piv_map_1118_11am.html')
# gmap.draw('sin.html')