# -*- coding: utf-8 -*-
"""
Created on Wed May 29 13:19:11 2024

@author: nichm
"""

import mysql.connector
import math
import numpy as np
import pandas as pd

db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'admin123',
    'database': 'new_schema'
}

horizontal_accuracy = 0.0141
vertical_accuracy = 0.01205

def euclidean_distance(lat1, lon1, lat2, lon2):
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)

    R = 6371000  # Radius of the Earth in meters

    delta_lat = lat2_rad - lat1_rad
    delta_lon = lon2_rad - lon1_rad

    distance = math.sqrt((delta_lat * R)**2 + (delta_lon * R * math.cos((lat1_rad + lat2_rad) / 2))**2)
    distance_cm = distance * 100

    return distance_cm

def fetch_gnss_table_names():
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

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

def fetch_all_gps_data(table_name):
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        query = f"SELECT ts, fix_type, latitude, longitude, hacc, vacc, msl, sat_num FROM {table_name}"
        cursor.execute(query)
        rows = cursor.fetchall()

        columns = ['ts', 'fix_type', 'latitude', 'longitude', 'hacc', 'vacc', 'msl', 'sat_num']
        all_data = pd.DataFrame(rows, columns=columns)
        return all_data

    except mysql.connector.Error as error:
        print(f"Error fetching GPS data from {table_name}: {error}")
        return pd.DataFrame()

    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()

def fetch_base_name_for_rover(rover_name):
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        query = "SELECT base_name FROM rover_reference_point WHERE LEFT(base_name, 3) = %s"
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

def fetch_reference_coordinates(base_name):
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        query = "SELECT latitude, longitude FROM rover_reference_point WHERE base_name = %s"
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

def prepare_and_apply_sanity_filters(df, horizontal_accuracy, vertical_accuracy):
    df['msl'] = np.round(df['msl'], 2)

    df = df[(df['fix_type'] == 2) & (df['sat_num'] > 28)]

    if df.empty:
        return df

    df = df[(df['hacc'] == horizontal_accuracy) & (df['vacc'] <= vertical_accuracy)]

    df = df.reset_index(drop=True).sort_values(by='ts', ascending=True, ignore_index=True)

    return df

def outlier_filter_for_latlon(df):
    df = df.copy()
    dfmean = df[['latitude', 'longitude']].rolling(window=10, min_periods=1).mean()
    dfsd = df[['latitude', 'longitude']].rolling(window=10, min_periods=1).std()

    dfulimits = dfmean + dfsd
    dfllimits = dfmean - dfsd

    df['latitude'] = df['latitude'].where((df['latitude'] <= dfulimits['latitude']) & (df['latitude'] >= dfllimits['latitude']), np.nan)
    df['longitude'] = df['longitude'].where((df['longitude'] <= dfulimits['longitude']) & (df['longitude'] >= dfllimits['longitude']), np.nan)

    df = df.dropna(subset=['latitude', 'longitude'])

    return df

# def compute_and_update_displacement_from_previous(rover_name, ts, rover_distref_cm):
#     try:
#         connection = mysql.connector.connect(**db_config)
#         cursor = connection.cursor()

#         query = f"SELECT distance_from_reference FROM stored_dist_gnss_{rover_name} WHERE ts < %s ORDER BY ts DESC LIMIT 1"
#         cursor.execute(query, (ts,))
#         previous_displacement_cm = cursor.fetchone()

#         if previous_displacement_cm is not None:
#             previous_displacement_cm = previous_displacement_cm[0]
#             displacement_from_previous = abs(rover_distref_cm - previous_displacement_cm)

#             update_query = f"UPDATE stored_dist_gnss_{rover_name} SET displacement_from_previous = %s WHERE ts = %s"
#             cursor.execute(update_query, (displacement_from_previous, ts))
#             connection.commit()
#             print("Displacement from previous updated successfully.")
#         else:
#             print("No previous record found. Skipping update.")

#     except mysql.connector.Error as error:
#         print(f"Error computing and updating displacement_from_previous in the database: {error}")

#     finally:
#         if 'connection' in locals() and connection.is_connected():
#             cursor.close()
#             connection.close()

def compute_and_update_displacement_from_previous(rover_name, ts_end_str, rover_distref_cm):
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        query = f"SELECT distance_from_reference FROM stored_dist_gnss_{rover_name} WHERE ts_end < %s ORDER BY ts_end DESC LIMIT 1"
        cursor.execute(query, (ts_end_str,))
        previous_displacement_cm = cursor.fetchone()

        if previous_displacement_cm is not None:
            previous_displacement_cm = previous_displacement_cm[0]
            displacement_from_previous = abs(rover_distref_cm - previous_displacement_cm)

            update_query = f"UPDATE stored_dist_gnss_{rover_name} SET displacement_from_previous = %s WHERE ts_end = %s"
            cursor.execute(update_query, (displacement_from_previous, ts_end_str))
            connection.commit()
            print("Displacement from previous updated successfully.")
        else:
            print("No previous record found. Skipping update.")

    except mysql.connector.Error as error:
        print(f"Error computing and updating displacement_from_previous in the database: {error}")

    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()



# def compute_velocity_cm_hr(rover_name, current_ts, time_difference_hours):
#     try:
#         connection = mysql.connector.connect(**db_config)
#         cursor = connection.cursor()

#         query = f"SELECT displacement_from_previous FROM stored_dist_gnss_{rover_name} WHERE ts = %s"
#         cursor.execute(query, (current_ts,))
#         displacement_from_previous = cursor.fetchone()

#         if displacement_from_previous is not None:
#             displacement_from_previous = displacement_from_previous[0]
#             velocity_cm_hr = displacement_from_previous / time_difference_hours
#             return velocity_cm_hr

#         else:
#             print("No displacement from previous found for", rover_name)
#             return None

#     except mysql.connector.Error as error:
#         print(f"Error computing velocity in database for {rover_name}: {error}")
#         return None

#     finally:
#         if 'connection' in locals() and connection.is_connected():
#             cursor.close()
#             connection.close()

def compute_velocity_cm_hr(rover_name, current_ts_end_str, time_difference_hours):
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        print(f"Computing velocity for {rover_name} at {current_ts_end_str}")

        query = f"SELECT displacement_from_previous FROM stored_dist_gnss_{rover_name} WHERE ts_end = %s"
        cursor.execute(query, (current_ts_end_str,))
        displacement_from_previous = cursor.fetchone()

        if displacement_from_previous is not None:
            displacement_from_previous = displacement_from_previous[0]
            velocity_cm_hr = displacement_from_previous / time_difference_hours
            return velocity_cm_hr

        else:
            print("No displacement from previous found for", rover_name)
            return None

    except mysql.connector.Error as error:
        print(f"Error computing velocity in database for {rover_name}: {error}")
        return None

    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()


def alert_on_velocity_threshold(rover_name, velocity_cm_hr):
    threshold_velocity_alert_2_cm_hr = 0.25
    threshold_velocity_alert_3_cm_hr = 1.8

    if velocity_cm_hr >= threshold_velocity_alert_3_cm_hr:
        print(f"Velocity Threshold Alert 3 exceeded for {rover_name}! Velocity: {velocity_cm_hr:.2f} cm/hr")
    elif velocity_cm_hr >= threshold_velocity_alert_2_cm_hr:
        print(f"Velocity Threshold Alert 2 exceeded for {rover_name}! Velocity: {velocity_cm_hr:.2f} cm/hr")
    else:
        print("-----no alert------")

def get_8_hour_windows(df):
    df['ts'] = pd.to_datetime(df['ts'])
    df['date'] = df['ts'].dt.date  # Extract date from timestamp
    df = df.set_index('ts')  # Set 'ts' as the index
    df = df.sort_index()

    windows = {}
    time_boundaries = [
        ('00:01', '08:00'),
        ('08:01', '16:00'),
        ('16:01', '00:00')
    ]

    for start, end in time_boundaries:
        start_time = pd.Timestamp(start).strftime('%H:%M:%S')
        end_time = pd.Timestamp(end).strftime('%H:%M:%S')

        for date, group in df.groupby('date'):
            group_window = group.between_time(start_time, end_time)
            if date not in windows:
                windows[date] = []
            if not group_window.empty:
                # Reset the index to retain the 'ts' column
                group_window = group_window.reset_index()
                windows[date].append(group_window)

    return windows

def get_4_hour_windows(df):
    df['ts'] = pd.to_datetime(df['ts'])
    df['date'] = df['ts'].dt.date  # Extract date from timestamp
    df = df.set_index('ts')  # Set 'ts' as the index
    df = df.sort_index()

    windows = {}
    time_boundaries = [
        ('00:01', '04:00'),
        ('04:01', '08:00'),
        ('08:01', '12:00'),
        ('12:01', '16:00'),
        ('16:01', '20:00'),
        ('20:01', '00:00')
    ]

    for start, end in time_boundaries:
        start_time = pd.Timestamp(start).strftime('%H:%M:%S')
        end_time = pd.Timestamp(end).strftime('%H:%M:%S')

        for date, group in df.groupby('date'):
            group_window = group.between_time(start_time, end_time)
            if date not in windows:
                windows[date] = []
            if not group_window.empty:
                # Reset the index to retain the 'ts' column
                group_window = group_window.reset_index()
                windows[date].append(group_window)

    return windows


# def check_threshold_and_alert():
#     gnss_table_names = fetch_gnss_table_names()

#     for table_name in gnss_table_names:
#         all_data = fetch_all_gps_data(table_name)
#         if all_data.empty:
#             continue

#         windows = get_4_hour_windows(all_data)
#         rover_name = table_name.replace('gnss_', '', 1)
#         base_name = fetch_base_name_for_rover(rover_name)
#         if not base_name:
#             continue

#         base_coords = fetch_reference_coordinates(base_name)
#         if not base_coords:
#             continue

#         base_lat, base_lon = base_coords

#         for date, window_list in windows.items():
#             for window_df in window_list:
#                 if window_df.empty:
#                     continue
        
#                 filtered_data = prepare_and_apply_sanity_filters(window_df, horizontal_accuracy, vertical_accuracy)
#                 if filtered_data.empty:
#                     continue
        
#                 filtered_data = outlier_filter_for_latlon(filtered_data)
#                 if filtered_data.empty:
#                     continue


#                 rover_coords = (filtered_data['latitude'].iloc[-1], filtered_data['longitude'].iloc[-1])
    
#                 rover_distref_cm = euclidean_distance(rover_coords[0], rover_coords[1], base_lat, base_lon)
    
#                 try:
#                     connection = mysql.connector.connect(**db_config)
#                     cursor = connection.cursor()
    
#                     end_time = filtered_data['ts'].iloc[-1]
    
#                     query = f"SELECT COUNT(*) FROM stored_dist_gnss_{rover_name} WHERE ts = %s"
#                     cursor.execute(query, (end_time,))
#                     count = cursor.fetchone()[0]
    
#                     if count == 0:
#                         query = f"INSERT INTO stored_dist_gnss_{rover_name} (ts, distance_from_reference) VALUES (%s, %s)"
#                         cursor.execute(query, (end_time, rover_distref_cm))
#                         connection.commit()
#                         print("Distance from reference stored successfully.")
#                     else:
#                         print("Duplicate entry found. Skipping insertion.")
    
#                     compute_and_update_displacement_from_previous(rover_name, end_time, rover_distref_cm)
    
#                     previous_ts_query = f"SELECT ts FROM stored_dist_gnss_{rover_name} WHERE ts < %s ORDER BY ts DESC LIMIT 1"
#                     cursor.execute(previous_ts_query, (end_time,))
#                     previous_ts_row = cursor.fetchone()
    
#                     if previous_ts_row is not None:
#                         previous_ts = previous_ts_row[0]
#                         time_difference_hours = (end_time - previous_ts).total_seconds() / 3600
#                         velocity_cm_hr = compute_velocity_cm_hr(rover_name, end_time, time_difference_hours)
    
#                         if velocity_cm_hr is not None:
#                             update_velocity_query = f"UPDATE stored_dist_gnss_{rover_name} SET velocity_cm_hr = %s WHERE ts = %s"
#                             cursor.execute(update_velocity_query, (velocity_cm_hr, end_time))
#                             connection.commit()
#                             print("Velocity updated successfully.")
#                             alert_on_velocity_threshold(rover_name, velocity_cm_hr)
    
#                 except mysql.connector.Error as error:
#                     print(f"Error: {error}")
    
#                 finally:
#                     if 'connection' in locals() and connection.is_connected():
#                         cursor.close()
#                         connection.close()

# def format_timestamp(ts):
#     return ts.strftime('%Y-%m-%d %H:%M:%S')


# def check_threshold_and_alert():
#     gnss_table_names = fetch_gnss_table_names()

#     for table_name in gnss_table_names:
#         all_data = fetch_all_gps_data(table_name)
#         if all_data.empty:
#             continue

#         windows = get_8_hour_windows(all_data)
#         rover_name = table_name.replace('gnss_', '', 1)
#         base_name = fetch_base_name_for_rover(rover_name)
#         if not base_name:
#             continue

#         base_coords = fetch_reference_coordinates(base_name)
#         if not base_coords:
#             continue

#         base_lat, base_lon = base_coords

#         for date, window_list in windows.items():
#             for window_df  in window_list:
#                 if window_df.empty:
#                     continue

#                 filtered_data = prepare_and_apply_sanity_filters(window_df, horizontal_accuracy, vertical_accuracy)
#                 if filtered_data.empty:
#                     continue

#                 filtered_data = outlier_filter_for_latlon(filtered_data)
#                 if filtered_data.empty:
#                     continue

#                 # Compute average latitude and longitude
#                 avg_latitude = filtered_data['latitude'].mean()
#                 avg_longitude = filtered_data['longitude'].mean()

#                 # Use the average latitude and longitude for calculations
#                 rover_coords = (avg_latitude, avg_longitude)
                
#                 rover_distref_cm = euclidean_distance(avg_latitude, avg_longitude, base_lat, base_lon)

#                 try:
#                     connection = mysql.connector.connect(**db_config)
#                     cursor = connection.cursor()

#                     end_time = filtered_data['ts'].iloc[-1]

#                     # Convert Python timestamp to MySQL-compatible string format
#                     end_time_str = format_timestamp(end_time)

#                     query = f"SELECT COUNT(*) FROM stored_dist_gnss_{rover_name} WHERE ts = %s"
#                     cursor.execute(query, (end_time_str,))
#                     count = cursor.fetchone()[0]

#                     if count == 0:
#                         query = f"INSERT INTO stored_dist_gnss_{rover_name} (ts, ts_written, distance_from_reference, displacement_from_previous) VALUES (%s, NOW(), %s, NULL)"
#                         cursor.execute(query, (end_time_str, rover_distref_cm))
#                         connection.commit()
#                         print("Distance from reference stored successfully.")
#                     else:
#                         print("Duplicate entry found. Skipping insertion.")

#                     compute_and_update_displacement_from_previous(rover_name, end_time, rover_distref_cm)

#                     previous_ts_query = f"SELECT ts FROM stored_dist_gnss_{rover_name} WHERE ts < %s ORDER BY ts DESC LIMIT 1"
#                     cursor.execute(previous_ts_query, (end_time_str,))
#                     previous_ts_row = cursor.fetchone()

#                     if previous_ts_row is not None:
#                         previous_ts = previous_ts_row[0]
#                         time_difference_hours = (end_time - previous_ts).total_seconds() / 3600
#                         velocity_cm_hr = compute_velocity_cm_hr(rover_name, end_time, time_difference_hours)

#                         if velocity_cm_hr is not None:
#                             update_velocity_query = f"UPDATE stored_dist_gnss_{rover_name} SET velocity_cm_hr = %s WHERE ts = %s"
#                             cursor.execute(update_velocity_query, (velocity_cm_hr, end_time))
#                             connection.commit()
#                             print("Velocity updated successfully.")
#                             alert_on_velocity_threshold(rover_name, velocity_cm_hr)

#                 except mysql.connector.Error as error:
#                     print(f"Error: {error}")

#                 finally:
#                     if 'connection' in locals() and connection.is_connected():
#                         cursor.close()
#                         connection.close()


# def check_threshold_and_alert():
#     gnss_table_names = fetch_gnss_table_names()

#     for table_name in gnss_table_names:
#         all_data = fetch_all_gps_data(table_name)
#         if all_data.empty:
#             continue

#         windows = get_8_hour_windows(all_data)
#         rover_name = table_name.replace('gnss_', '', 1)
#         base_name = fetch_base_name_for_rover(rover_name)
#         if not base_name:
#             continue

#         base_coords = fetch_reference_coordinates(base_name)
#         if not base_coords:
#             continue

#         base_lat, base_lon = base_coords

#         for date, window_list in windows.items():
#             for window_df in window_list:
#                 if window_df.empty:
#                     continue

#                 filtered_data = prepare_and_apply_sanity_filters(window_df, horizontal_accuracy, vertical_accuracy)
#                 if filtered_data.empty:
#                     continue

#                 filtered_data = outlier_filter_for_latlon(filtered_data)
#                 if filtered_data.empty:
#                     continue

#                 # Compute average latitude and longitude
#                 avg_latitude = filtered_data['latitude'].mean()
#                 avg_longitude = filtered_data['longitude'].mean()

#                 # Use the average latitude and longitude for calculations
#                 rover_coords = (avg_latitude, avg_longitude)

#                 rover_distref_cm = euclidean_distance(avg_latitude, avg_longitude, base_lat, base_lon)

#                 try:
#                     connection = mysql.connector.connect(**db_config)
#                     cursor = connection.cursor()

#                     end_time = filtered_data['ts'].iloc[-1]

#                     # Convert Python timestamp to MySQL-compatible string format
#                     end_time_str = end_time.strftime('%Y-%m-%d %H:%M:%S')
#                     print(f"End time (string format): {end_time_str}")

#                     query = f"SELECT COUNT(*) FROM stored_dist_gnss_{rover_name} WHERE ts = %s"
#                     cursor.execute(query, (end_time_str,))
#                     count = cursor.fetchone()[0]

#                     if count == 0:
#                         query = f"INSERT INTO stored_dist_gnss_{rover_name} (ts, ts_written, distance_from_reference, displacement_from_previous) VALUES (%s, NOW(), %s, NULL)"
#                         cursor.execute(query, (end_time_str, rover_distref_cm))
#                         connection.commit()
#                         print("Distance from reference stored successfully.")
#                     else:
#                         print("Duplicate entry found. Skipping insertion.")

#                     compute_and_update_displacement_from_previous(rover_name, end_time_str, rover_distref_cm)

#                     previous_ts_query = f"SELECT ts FROM stored_dist_gnss_{rover_name} WHERE ts < %s ORDER BY ts DESC LIMIT 1"
#                     cursor.execute(previous_ts_query, (end_time_str,))
#                     previous_ts_row = cursor.fetchone()

#                     if previous_ts_row is not None:
#                         previous_ts_str = previous_ts_row[0]
#                         previous_ts = pd.to_datetime(previous_ts_str)
#                         time_difference_hours = (end_time - previous_ts).total_seconds() / 3600
#                         velocity_cm_hr = compute_velocity_cm_hr(rover_name, end_time_str, time_difference_hours)

#                         if velocity_cm_hr is not None:
#                             update_velocity_query = f"UPDATE stored_dist_gnss_{rover_name} SET velocity_cm_hr = %s WHERE ts = %s"
#                             cursor.execute(update_velocity_query, (velocity_cm_hr, end_time_str))
#                             connection.commit()
#                             print("Velocity updated successfully.")
#                             alert_on_velocity_threshold(rover_name, velocity_cm_hr)

#                 except mysql.connector.Error as error:
#                     print(f"Error: {error}")

#                 finally:
#                     if 'connection' in locals() and connection.is_connected():
#                         cursor.close()
#                         connection.close()

def check_threshold_and_alert():
    gnss_table_names = fetch_gnss_table_names()

    for table_name in gnss_table_names:
        all_data = fetch_all_gps_data(table_name)
        if all_data.empty:
            continue

        #windows = get_4_hour_windows(all_data)
        windows = get_8_hour_windows(all_data)
        rover_name = table_name.replace('gnss_', '', 1)
        base_name = fetch_base_name_for_rover(rover_name)
        if not base_name:
            continue

        base_coords = fetch_reference_coordinates(base_name)
        if not base_coords:
            continue

        base_lat, base_lon = base_coords

        for date, window_list in windows.items():
            for window_df in window_list:
                if window_df.empty:
                    continue

                filtered_data = prepare_and_apply_sanity_filters(window_df, horizontal_accuracy, vertical_accuracy)
                if filtered_data.empty:
                    continue

                filtered_data = outlier_filter_for_latlon(filtered_data)
                if filtered_data.empty:
                    continue

                # Compute average latitude and longitude
                avg_latitude = filtered_data['latitude'].mean()
                avg_longitude = filtered_data['longitude'].mean()

                # Use the average latitude and longitude for calculations
                rover_coords = (avg_latitude, avg_longitude)

                rover_distref_cm = euclidean_distance(avg_latitude, avg_longitude, base_lat, base_lon)

                try:
                    connection = mysql.connector.connect(**db_config)
                    cursor = connection.cursor()

                    ts_start = filtered_data['ts'].iloc[0]
                    ts_end = filtered_data['ts'].iloc[-1]

                    # Convert Python timestamp to MySQL-compatible string format
                    ts_start_str = ts_start.strftime('%Y-%m-%d %H:%M:%S')
                    ts_end_str = ts_end.strftime('%Y-%m-%d %H:%M:%S')
                    print(f"Window start time (string format): {ts_start_str}")
                    print(f"Window end time (string format): {ts_end_str}")

                    query = f"SELECT COUNT(*) FROM stored_dist_gnss_{rover_name} WHERE ts_start = %s AND ts_end = %s"
                    cursor.execute(query, (ts_start_str, ts_end_str))
                    count = cursor.fetchone()[0]

                    if count == 0:
                        query = f"INSERT INTO stored_dist_gnss_{rover_name} (ts_start, ts_end, ts_written, distance_from_reference, displacement_from_previous) VALUES (%s, %s, NOW(), %s, NULL)"
                        cursor.execute(query, (ts_start_str, ts_end_str, rover_distref_cm))
                        connection.commit()
                        print("Distance from reference stored successfully.")
                    else:
                        print("Duplicate entry found. Skipping insertion.")

                    previous_ts_query = f"SELECT ts_end FROM stored_dist_gnss_{rover_name} WHERE ts_end < %s ORDER BY ts_end DESC LIMIT 1"
                    cursor.execute(previous_ts_query, (ts_end_str,))
                    previous_ts_row = cursor.fetchone()

                    if previous_ts_row is not None:
                        previous_ts = previous_ts_row[0]
                        time_difference_hours = 8  # Explicitly setting to 8 hours

                        compute_and_update_displacement_from_previous(rover_name, ts_end_str, rover_distref_cm)

                        velocity_cm_hr = compute_velocity_cm_hr(rover_name, ts_end_str, time_difference_hours)

                        if velocity_cm_hr is not None:
                            update_velocity_query = f"UPDATE stored_dist_gnss_{rover_name} SET velocity_cm_hr = %s WHERE ts_end = %s"
                            cursor.execute(update_velocity_query, (velocity_cm_hr, ts_end_str))
                            connection.commit()
                            print("Velocity updated successfully.")
                            alert_on_velocity_threshold(rover_name, velocity_cm_hr)

                except mysql.connector.Error as error:
                    print(f"Error: {error}")

                finally:
                    if 'connection' in locals() and connection.is_connected():
                        cursor.close()
                        connection.close()


# Schedule this function to run every 4 hours using a scheduling library like 'schedule' or cron jobs in a real deployment.
check_threshold_and_alert()
