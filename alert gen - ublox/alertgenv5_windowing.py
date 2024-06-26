import mysql.connector
import math
import numpy as np
import pandas as pd
import time
from datetime import datetime, time, timedelta


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
    threshold_velocity_alert_3_cm_hr = 1.80

    if velocity_cm_hr >= threshold_velocity_alert_3_cm_hr:
        print(f"Velocity Threshold Alert 3 exceeded for {rover_name}! Velocity: {velocity_cm_hr:.3f} cm/hr")
    elif velocity_cm_hr >= threshold_velocity_alert_2_cm_hr:
        print(f"Velocity Threshold Alert 2 exceeded for {rover_name}! Velocity: {velocity_cm_hr:.3f} cm/hr")
    else:
        print("-----no alert------")

def get_8_hour_windows(df):
    df['ts'] = pd.to_datetime(df['ts'])
    df['date'] = df['ts'].dt.date  # Extract date from timestamp

    windows = {}
    time_boundaries = [
        ('00:00:00', '07:59:59'),
        ('08:00:00', '15:59:59'),
        ('16:00:00', '23:59:59')
    ]

    for start, end in time_boundaries:
        start_time = pd.to_datetime(start).time()
        end_time = pd.to_datetime(end).time()

        for date, group in df.groupby('date'):
            group_window = group[(group['ts'].dt.time >= start_time) & (group['ts'].dt.time <= end_time)]
            if not group_window.empty:
                if date not in windows:
                    windows[date] = []
                windows[date].append(group_window)

    return windows


def is_processed(rover_name, ts_end_str):
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        query = f"SELECT COUNT(*) FROM stored_dist_gnss_{rover_name} WHERE ts_end = %s"
        cursor.execute(query, (ts_end_str,))
        count = cursor.fetchone()[0]

        return count > 0

    except mysql.connector.Error as error:
        print(f"Error checking if entry is processed for {rover_name} at {ts_end_str}: {error}")
        return False

    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()

##all data fetching:
def check_threshold_and_alert():
    gnss_table_names = fetch_gnss_table_names()

    for table_name in gnss_table_names:
        all_data = fetch_all_gps_data(table_name) ##--> fetching all stored data from table and not gathering latest/recent
        if all_data.empty:
            continue

        #windows = get_4_hour_windows(all_data)
        windows = get_8_hour_windows(all_data) #deactivate function if running realtime
        rover_name = table_name.replace('gnss_', '', 1)
        base_name = fetch_base_name_for_rover(rover_name)
        if not base_name:
            continue

        base_coords = fetch_reference_coordinates(base_name)
        if not base_coords:
            continue

        base_lat, base_lon = base_coords

        sorted_dates = sorted(windows.keys())
        for date in sorted_dates:
            window_list = windows[date]
            for window_df in window_list:
                if window_df.empty:
                    continue

                filtered_data = prepare_and_apply_sanity_filters(window_df, horizontal_accuracy, vertical_accuracy)
                if filtered_data.empty:
                    continue

                filtered_data = outlier_filter_for_latlon(filtered_data)
                if filtered_data.empty:
                    continue
                
                # Sort the data by ts_start
                filtered_data = filtered_data.sort_values(by='ts', ascending=True)

                # Compute average latitude and longitude
                avg_latitude = filtered_data['latitude'].mean()
                avg_longitude = filtered_data['longitude'].mean()

                # Use the average latitude and longitude for calculations
                rover_coords = (avg_latitude, avg_longitude)
                print("avg_latitude: ", avg_latitude, " ", "avg_longitude: ", avg_longitude)

                rover_distref_cm = euclidean_distance(avg_latitude, avg_longitude, base_lat, base_lon)

                try:
                    connection = mysql.connector.connect(**db_config)
                    cursor = connection.cursor()

                    ts_start = window_df['ts'].iloc[0]
                    ts_end = window_df['ts'].iloc[-1]

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

##fetching data timedelta 8hrs
def data_exists_in_stored_table(rover_name, ts_start, ts_end):
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        query = f"""
        SELECT COUNT(*) FROM stored_dist_gnss_{rover_name}
        WHERE ts_start = %s AND ts_end = %s
        """
        cursor.execute(query, (ts_start, ts_end))
        count = cursor.fetchone()[0]

        return count > 0

    except mysql.connector.Error as error:
        print(f"Error checking data existence in stored_dist_gnss_{rover_name}: {error}")
        return False

    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()

def fetch_recent_gps_data(table_name, hours=8):
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        end_time = datetime.now()
        start_time = end_time - timedelta(hours=hours)

        query = f"SELECT ts, fix_type, latitude, longitude, hacc, vacc, msl, sat_num FROM {table_name} WHERE ts >= %s AND ts <= %s"
        cursor.execute(query, (start_time, end_time))
        rows = cursor.fetchall()

        columns = ['ts', 'fix_type', 'latitude', 'longitude', 'hacc', 'vacc', 'msl', 'sat_num']
        recent_data = pd.DataFrame(rows, columns=columns)

        if recent_data.empty:
            return recent_data
        
        last_ts = recent_data['ts'].iloc[-1]

        if last_ts.hour >= 16:
            # If the last timestamp hour is 16 or later, filter data for the time boundary 16:01-23:59
            start_boundary = datetime.combine(last_ts.date(), time(16, 0))
            end_boundary = datetime.combine(last_ts.date(), time(23, 59))
        elif last_ts.hour >= 8:
            # If the last timestamp hour is between 8 and 15, filter data for the time boundary 08:01-16:00
            start_boundary = datetime.combine(last_ts.date(), time(8, 0))
            end_boundary = datetime.combine(last_ts.date(), time(15, 59))
        else:
            # If the last timestamp hour is before 8, filter data for the time boundary 00:01-08:00
            start_boundary = datetime.combine(last_ts.date(), time(0, 0))
            end_boundary = datetime.combine(last_ts.date(), time(7, 59))

        boundary_data = recent_data[
            (recent_data['ts'] >= start_boundary) &
            (recent_data['ts'] <= end_boundary)
        ]

        if boundary_data.empty:
            return boundary_data

        # Check if data already exists to avoid duplicates
        ts_start_str = boundary_data['ts'].iloc[0].strftime('%Y-%m-%d %H:%M:%S')
        ts_end_str = boundary_data['ts'].iloc[-1].strftime('%Y-%m-%d %H:%M:%S')
        rover_name = table_name.replace('gnss_', '', 1)

        if data_exists_in_stored_table(rover_name, ts_start_str, ts_end_str):
            print(f"Data for the period {ts_start_str} to {ts_end_str} already exists.")
            return pd.DataFrame()  # Return empty DataFrame to indicate no new data to process

        return boundary_data

    except mysql.connector.Error as error:
        print(f"Error fetching recent GPS data from {table_name}: {error}")
        return pd.DataFrame()

    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()
