import mysql.connector
import math
import numpy as np
import pandas as pd

pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.float_format', '{:,.9f}'.format)

db_config = {
    'host': '192.168.150.112',
    'user': 'pysys_local',
    'password': 'NaCAhztBgYZ3HwTkvHwwGVtJn5sVMFgg',
    'database': 'analysis_db'
}

connection = mysql.connector.connect(**db_config)
cursor = connection.cursor()


def haversine_distance(lat1, lon1, lat2, lon2):
    # Convert latitude and longitude from degrees to radians
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)

    # Haversine formula
    delta_lat = lat2_rad - lat1_rad
    delta_lon = lon2_rad - lon1_rad
    a = math.sin(delta_lat / 2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    # Radius of the Earth in meters (use 6371000 meters for average Earth radius)
    earth_radius = 6371000

    # Calculate the distance
    distance = earth_radius * c
    return distance #in meters


def fetch_gnss_table_names():
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        # Fetch table names in the current database that start with 'gnss_'
        query = "SHOW TABLES LIKE 'gnss\_%'"
        cursor.execute(query)
        table_rows = cursor.fetchall()

        gnss_table_names = [table_name[0] for table_name in table_rows]
        return gnss_table_names

    except mysql.connector.Error as error:
        print(f"Error fetching GNSS table names: {error}")
        return []

    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()
            
            
def fetch_latest_gps_data(table_name):
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        # Construct query to fetch the most recent GPS data from the specified table
        query = f"SELECT ts, fix_type, latitude, longitude, hacc, vacc, msl, sat_num FROM {table_name} ORDER BY ts DESC LIMIT 1"
        cursor.execute(query)
        row = cursor.fetchone()

        if not row:
            return None

        ts, fix_type, latitude, longitude, hacc, vacc, msl, sat_num = row
        return ts, fix_type, latitude, longitude, hacc, vacc, msl, sat_num

    except mysql.connector.Error as error:
        print(f"Error fetching GPS data from {table_name}: {error}")
        return None

    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()
        

def fetch_base_name_for_rover(rover_name):
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        # Fetch base name based on the first 3 characters of rover_name
        query = "SELECT base_name FROM base_stations WHERE LEFT(base_name, 3) = %s"
        cursor.execute(query, (rover_name[:3],))
        row = cursor.fetchone()

        if not row:
            return None

        return row[0]

    except mysql.connector.Error as error:
        print(f"Error fetching base name for {rover_name}: {error}")
        return None

    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()

def fetch_base_coordinates(base_name):
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        # Fetch base coordinates based on base_name
        query = "SELECT latitude, longitude FROM base_stations WHERE base_name = %s"
        cursor.execute(query, (base_name,))
        row = cursor.fetchone()

        if not row:
            return None

        return row[0], row[1]

    except mysql.connector.Error as error:
        print(f"Error fetching base coordinates for {base_name}: {error}")
        return None

    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()

# def check_threshold_and_alert():
#     # Fetch all GNSS table names in the database
#     gnss_table_names = fetch_gnss_table_names()

#     for table_name in gnss_table_names:
#         timestamp, _, lat, lon = fetch_latest_gps_data(table_name)  # Ignore rover_name from fetch_latest_gps_data

#         if not (timestamp and lat and lon):
#             continue  # Skip if no valid GPS data for this table

#         # Extract rover name from table_name (assuming format is 'gnss_rovername')
#         rover_name = table_name.replace('gnss_', '', 1)

#         # Fetch base name for the rover
#         base_name = fetch_base_name_for_rover(rover_name)

#         if not base_name:
#             continue  # Base name not found for this rover

#         # Fetch base coordinates using the base name
#         base_lat, base_lon = fetch_base_coordinates(base_name)

#         if not (base_lat and base_lon):
#             continue  # Base station coordinates not found

#         rover_coords = (lat, lon)
#         base_coords = (base_lat, base_lon)
#         displacement_km = calculate_displacement(rover_coords, base_coords)

#         if displacement_km >= threshold_km:
#             print(f"Threshold ({threshold_km} km) exceeded for {rover_name}! Displacement: {displacement_km} km")
#             # Code to trigger alert (e.g., send email, push notification, etc.)

# def check_threshold_and_alert():
#     # Fetch all GNSS table names in the database
#     gnss_table_names = fetch_gnss_table_names()

#     for table_name in gnss_table_names:
#         ts, fix_type, latitude, longitude, hacc, vacc, msl, sat_num = fetch_latest_gps_data(table_name)  # Ignore rover_name from fetch_latest_gps_data

#         if not (ts and latitude and longitude):
#             continue  # Skip if no valid GPS data for this table

#         # Extract rover name from table_name (assuming format is 'gnss_rovername')
#         rover_name = table_name.replace('gnss_', '', 1)

#         # Fetch base name for the rover
#         base_name = fetch_base_name_for_rover(rover_name)

#         if not base_name:
#             continue  # Base name not found for this rover

#         # Fetch base coordinates using the base name
#         base_lat, base_lon = fetch_base_coordinates(base_name)

#         if not (base_lat and base_lon):
#             continue  # Base station coordinates not found

#         rover_coords = (latitude, longitude)
#         base_coords = (base_lat, base_lon)
#         displacement_cm = haversine_distance(rover_coords[0], rover_coords[1], base_coords[0], base_coords[1])

#         # Check threshold
#         threshold_velocity_alert_2_cm_hr = 0.25
#         threshold_velocity_alert_3_cm_hr = 1.8
#         # Assuming you have time data as well for velocity calculation
#         time_difference_hours = 15/60  # Calculate time difference between current and previous GPS data

#         # Calculate velocity in cm/hr
#         velocity_cm_hr = displacement_cm / time_difference_hours

#         # Check velocity against thresholds
#         if velocity_cm_hr >= threshold_velocity_alert_3_cm_hr:
#             print(f"Velocity Threshold Alert 3 exceeded for {rover_name}! Velocity: {velocity_cm_hr:.2f} cm/hr")
#             # Code to trigger alert (e.g., send email, push notification, etc.)
#         elif velocity_cm_hr >= threshold_velocity_alert_2_cm_hr:
#             print(f"Velocity Threshold Alert 2 exceeded for {rover_name}! Velocity: {velocity_cm_hr:.2f} cm/hr")
#             # Code to trigger alert (e.g., send email, push notification, etc.)
            

#filters to be applied
def prepare_data(data):
    new_df = data.copy()

    # Round 'msl' column to 2 decimal places
    new_df['msl'] = np.round(new_df['msl'], 2)

    # Apply fix_type and sat_num filters
    new_df = new_df.loc[(new_df['fix_type'] == 2) & (new_df['sat_num'] > 28)]

    print('new_df fix=2, satnum>28:', len(new_df))
    return new_df

def apply_accuracy_filters(df, horizontal_accuracy, vertical_accuracy):
    # Apply horizontal and vertical accuracy filters
    df = df.loc[(df['hacc'] == horizontal_accuracy) & (df['vacc'] <= vertical_accuracy)]
    df = df.reset_index(drop=True).sort_values(by='ts', ascending=True, ignore_index=True)

    print('new_df hacc vacc:', len(df))
    return df

def filter_decimal_precision(df):
    # Filter for complete decimal precision in latitude and longitude
    df = df[(df['latitude'].astype(str).str[-10] == '.') & (df['longitude'].astype(str).str[-10] == '.')]

    print('new_df complete deci:', len(df))
    return df

def outlier_filter(df):
    dff = df.copy()

    dfmean = dff[['latitude','longitude','msl_rounded']].\
            rolling(min_periods=1,window=6,center=False).mean()
    dfsd = dff[['latitude','longitude','msl_rounded']].\
            rolling(min_periods=1,window=6,center=False).std()

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

df_outlier_applied = outlier_filter(df)
print(df_outlier_applied)
#end of filters




####applying filters in check threshold
#all in 1 sanity check filter

horizontal_accuracy = 0.0141
vertical_accuracy = 0.01205

#sanity filter accepts datafra,
def prepare_and_apply_sanity_filters(data, horizontal_accuracy, vertical_accuracy):
    new_df = data.copy()

    # Round 'msl' column to 2 decimal places
    new_df['msl'] = np.round(new_df['msl'], 2)

    # Apply fix_type and sat_num filters
    new_df = new_df.loc[(new_df['fix_type'] == 2) & (new_df['sat_num'] > 28)]

    print('new_df fix=2, satnum>28:', len(new_df))

    # Apply horizontal and vertical accuracy filters
    new_df = new_df.loc[(new_df['hacc'] == horizontal_accuracy) & (new_df['vacc'] <= vertical_accuracy)]
    new_df = new_df.reset_index(drop=True).sort_values(by='ts', ascending=True, ignore_index=True)

    print('new_df hacc vacc:', len(new_df))

    # Filter for complete decimal precision in latitude and longitude
    # new_df = new_df[(new_df['latitude'].astype(str).str[-10] == '.') & (new_df['longitude'].astype(str).str[-10] == '.')]

    # print('new_df complete deci:', len(new_df))

    return new_df

def format_float(val):
    if isinstance(val, float):
        return '{:.9f}'.format(val).rstrip('0').rstrip('.')
    return val


#sanity filter accepts tuples
def prepare_and_apply_sanity_filters(tuple_data, horizontal_accuracy, vertical_accuracy):
    # Convert tuple to DataFrame
    columns = ['ts', 'fix_type', 'latitude', 'longitude', 'hacc', 'vacc', 'msl', 'sat_num']
    data = pd.DataFrame([tuple_data], columns=columns)

    # Round 'msl' column to 2 decimal places
    data['msl'] = np.round(data['msl'], 2)

    # Apply fix_type and sat_num filters
    data = data.loc[(data['fix_type'] == 2) & (data['sat_num'] > 28)]

    # Apply horizontal and vertical accuracy filters
    data = data.loc[(data['hacc'] == horizontal_accuracy) & (data['vacc'] <= vertical_accuracy)]
    data = data.reset_index(drop=True).sort_values(by='ts', ascending=True, ignore_index=True)

    # Return the filtered data as a tuple
    filtered_tuple = tuple(data.iloc[0])
    return filtered_tuple


def outlier_filter_for_msl(df):
    dff = df.copy()

    dfmean = dff[['msl']].\
            rolling(min_periods=1,window=10,center=False).mean()
    dfsd = dff[['msl']].\
            rolling(min_periods=1,window=10,center=False).std()

    dfulimits = dfmean + (1*dfsd)
    dfllimits = dfmean - (1*dfsd)

    dff.msl[(dff.msl > dfulimits.msl) | \
        (dff.msl < dfllimits.msl)] = np.nan

    dflogic = dff.msl
    dff = dff[dflogic.notnull()]

    return dff
    

def outlier_filter_for_latlon(df):
    dff = df.copy()

    dfmean = dff[['latitude','longitude']].\
            rolling(min_periods=1,window=10,center=False).mean()
    dfsd = dff[['latitude','longitude']].\
            rolling(min_periods=1,window=10,center=False).std()

    dfulimits = dfmean + (1*dfsd)
    dfllimits = dfmean - (1*dfsd)

    dff.latitude[(dff.latitude > dfulimits.latitude) | \
        (dff.latitude < dfllimits.latitude)] = np.nan
    dff.longitude[(dff.longitude > dfulimits.longitude) | \
        (dff.longitude < dfllimits.longitude)] = np.nan

    dflogic = dff.latitude * dff.longitude
    dff = dff[dflogic.notnull()]

    return dff


def fetch_window_data(table_name, timestamp, window_size=10):
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        # Construct query to fetch the window of data
        query = f"SELECT ts, fix_type, latitude, longitude, hacc, vacc, msl, sat_num FROM {table_name} WHERE ts <= %s ORDER BY ts DESC LIMIT %s"
        cursor.execute(query, (timestamp, window_size))
        rows = cursor.fetchall()

        # Convert fetched data into DataFrame
        columns = ['ts', 'fix_type', 'latitude', 'longitude', 'hacc', 'vacc', 'msl', 'sat_num']
        window_data = pd.DataFrame(rows, columns=columns)

        return window_data

    except mysql.connector.Error as error:
        print(f"Error fetching window data from {table_name}: {error}")
        return None

    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()
            

# def check_threshold_and_alert():
#     # Fetch all GNSS table names in the database
#     gnss_table_names = fetch_gnss_table_names()

#     for table_name in gnss_table_names:
#         ts, fix_type, latitude, longitude, hacc, vacc, msl, sat_num = fetch_latest_gps_data(table_name)  # Ignore rover_name from fetch_latest_gps_data
#         # latest_gps_data = prepare_and_apply_sanity_filters(pd.DataFrame([fetch_latest_gps_data(table_name)], columns=columns), horizontal_accuracy, vertical_accuracy)
#         # formatted_df_latest_gps_data = latest_gps_data.applymap(format_float)

#         # columns_to_convert = ['latitude', 'longitude', 'hacc', 'vacc', 'msl']
#         # formatted_df_latest_gps_data[columns_to_convert] = formatted_df_latest_gps_data[columns_to_convert].apply(pd.to_numeric, errors='coerce')
        
#         # sanity_filtered_data = prepare_and_apply_sanity_filters((ts, fix_type, latitude, longitude, hacc, vacc, msl, sat_num), horizontal_accuracy, vertical_accuracy)
#         # window_data = fetch_window_data(table_name, ts, window_size=10)
        
#         # # Apply outlier filter to window data
#         # filtered_window_data = outlier_filter_for_latlon(window_data)
        
#         # # Check if the latest fetched data (index 0) is an outlier
#         # latest_fetched_data = filtered_window_data.iloc[0]
        
#         # # Check if any of the values in the latest fetched data are NaN (indicating they were filtered out)
#         # if latest_fetched_data.isnull().values.any():
#         #     print("Latest fetched data is an outlier and was filtered out.")
#         # else:
#         #     print("Latest fetched data is not an outlier.")
#         #     # Proceed with distance computation using the latest fetched data
#         #     # Calculate distance...
#         #     displacement_cm = haversine_distance(rover_coords[0], rover_coords[1], base_coords[0], base_coords[1])

#         if not (ts and latitude and longitude):
#             continue  # Skip if no valid GPS data for this table

#         # Extract rover name from table_name (assuming format is 'gnss_rovername')
#         rover_name = table_name.replace('gnss_', '', 1)

#         # Fetch base name for the rover
#         base_name = fetch_base_name_for_rover(rover_name)

#         if not base_name:
#             continue  # Base name not found for this rover

#         # Fetch base coordinates using the base name
#         base_lat, base_lon = fetch_base_coordinates(base_name)

#         if not (base_lat and base_lon):
#             continue  # Base station coordinates not found

#         rover_coords = (latitude, longitude)
#         # # rover_coords = (latest_gps_data.latitude, latest_gps_data.longitude)
#         # # rover_coords = (formatted_df_latest_gps_data.latitude, formatted_df_latest_gps_data.longitude)
#         base_coords = (base_lat, base_lon)
#         # # displacement_cm = haversine_distance(rover_coords[0], rover_coords[1], base_coords[0], base_coords[1])

#         sanity_filtered_data = prepare_and_apply_sanity_filters((ts, fix_type, latitude, longitude, hacc, vacc, msl, sat_num), horizontal_accuracy, vertical_accuracy)
#         window_data = fetch_window_data(table_name, ts, window_size=10)
        
#         # Apply outlier filter to window data
#         filtered_window_data = outlier_filter_for_latlon(window_data)
        
#         # Check if the latest fetched data (index 0) is an outlier
#         latest_fetched_data = filtered_window_data.iloc[0]
        
#         # Check if any of the values in the latest fetched data are NaN (indicating they were filtered out)
#         if latest_fetched_data.isnull().values.any():
#             print("Latest fetched data is an outlier and was filtered out.")
#         else:
#             print("Latest fetched data is not an outlier.")
#             # Proceed with distance computation using the latest fetched data
#             # Calculate distance...
#             displacement_cm = haversine_distance(rover_coords[0], rover_coords[1], base_coords[0], base_coords[1])


#         # Check threshold
#         threshold_velocity_alert_2_cm_hr = 0.25
#         threshold_velocity_alert_3_cm_hr = 1.8
#         # Assuming you have time data as well for velocity calculation
#         time_difference_hours = 15/60  # Calculate time difference between current and previous GPS data

#         # Calculate velocity in cm/hr
#         velocity_cm_hr = displacement_cm / time_difference_hours

#         # Check velocity against thresholds
#         if velocity_cm_hr >= threshold_velocity_alert_3_cm_hr:
#             print(f"Velocity Threshold Alert 3 exceeded for {rover_name}! Velocity: {velocity_cm_hr:.2f} cm/hr")
#             # Code to trigger alert (e.g., send email, push notification, etc.)
#         elif velocity_cm_hr >= threshold_velocity_alert_2_cm_hr:
#             print(f"Velocity Threshold Alert 2 exceeded for {rover_name}! Velocity: {velocity_cm_hr:.2f} cm/hr")
#             # Code to trigger alert (e.g., send email, push notification, etc.)
            

#cleaner version:
def check_threshold_and_alert():
    gnss_table_names = fetch_gnss_table_names() # Fetch all GNSS table names in the database
    
    for table_name in gnss_table_names:
        ts, fix_type, latitude, longitude, hacc, vacc, msl, sat_num = fetch_latest_gps_data(table_name)  # Ignore rover_name from fetch_latest_gps_data
        sanity_filtered_data = prepare_and_apply_sanity_filters((ts, fix_type, latitude, longitude, hacc, vacc, msl, sat_num), horizontal_accuracy, vertical_accuracy)
        
        if not (ts and latitude and longitude):
           continue  # Skip if no valid GPS data for this table
           
        rover_name = table_name.replace('gnss_', '', 1) # Extract rover name from table_name
        base_name = fetch_base_name_for_rover(rover_name) # Fetch base name for the rover
        
        if not base_name:
            continue  # Base name not found for this rover
            
        base_lat, base_lon = fetch_base_coordinates(base_name)
        
        if not (base_lat and base_lon):
            continue  # Base station coordinates not found

        rover_coords = (latitude, longitude)
        base_coords = (base_lat, base_lon)
        
        window_data = fetch_window_data(table_name, ts, window_size=10) #fetch window data
        filtered_window_data = outlier_filter_for_latlon(window_data) # Apply outlier filter to window data
        latest_fetched_data = filtered_window_data.iloc[0] # Check if the latest fetched data (index 0) is an outlier
        
        # Check if any of the values in the latest fetched data are NaN (indicating they were filtered out)
        if latest_fetched_data.isnull().values.any():
            print("Latest fetched data is an outlier and was filtered out.")
        else:
            print("Latest fetched data is not an outlier.")
            # Proceed with distance computation using the latest fetched data
            # Calculate distance...
            displacement_cm = haversine_distance(rover_coords[0], rover_coords[1], base_coords[0], base_coords[1])
            
            # Check threshold
            threshold_velocity_alert_2_cm_hr = 0.25
            threshold_velocity_alert_3_cm_hr = 1.8
            # Assuming you have time data as well for velocity calculation
            time_difference_hours = 15/60  # Calculate time difference between current and previous GPS data
            
            # Calculate velocity in cm/hr
            velocity_cm_hr = displacement_cm / time_difference_hours
            
            # Check velocity against thresholds
            if velocity_cm_hr >= threshold_velocity_alert_3_cm_hr:
                print(f"Velocity Threshold Alert 3 exceeded for {rover_name}! Velocity: {velocity_cm_hr:.2f} cm/hr")
                # Code to trigger alert (e.g., send email, push notification, etc.)
            elif velocity_cm_hr >= threshold_velocity_alert_2_cm_hr:
                print(f"Velocity Threshold Alert 2 exceeded for {rover_name}! Velocity: {velocity_cm_hr:.2f} cm/hr")
                # Code to trigger alert (e.g., send email, push notification, etc.)
                
                
