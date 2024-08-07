import mysql.connector
import re
import math


# def calculate_euclidean_distance(lat1, lon1, lat2, lon2):
#     # Radius of the Earth in meters
#     R = 6371000

#     # Convert latitude and longitude from degrees to radians
#     lat1_rad = math.radians(lat1)
#     lon1_rad = math.radians(lon1)
#     lat2_rad = math.radians(lat2)
#     lon2_rad = math.radians(lon2)

#     # Convert to Cartesian coordinates
#     x1 = R * math.cos(lat1_rad) * math.cos(lon1_rad)
#     y1 = R * math.cos(lat1_rad) * math.sin(lon1_rad)
#     x2 = R * math.cos(lat2_rad) * math.cos(lon2_rad)
#     y2 = R * math.cos(lat2_rad) * math.sin(lon2_rad)

#     # Calculate Euclidean distance
#     distance = math.sqrt((x2 - x1)**2 + (y2 - y1)**2)

#     return distance

def haversine_distance(lat1, lon1, lat2, lon2):
    # Convert latitude and longitude from degrees to radians
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)

    # Haversine formula
    delta_lat = lat2_rad - lat1_rad
    delta_lon = lon2_rad - lon1_rad
    a = math.sin(delta_lat / 2)**2 + math.cos(lat1_rad) * \
            math.cos(lat2_rad) * math.sin(delta_lon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    # Radius of the Earth in meters 
    # Use 6371000 meters for average Earth radius
    earth_radius = 6371000

    # Calculate the distance
    distance = earth_radius * c
    return distance #in meters


def euclidean_distance(lat1, lon1, lat2, lon2): #without radius.
    # Convert latitude and longitude from degrees to radians
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)

    # Calculate differences in coordinates
    delta_lat = lat2_rad - lat1_rad
    delta_lon = lon2_rad - lon1_rad

    # Calculate Euclidean distance using the formula:
    # distance = sqrt((delta_lat)^2 + (delta_lon)^2) * earth_radius
    earth_radius = 6371000  # Earth's radius in meters
    distance = math.sqrt(delta_lat**2 + delta_lon**2) * earth_radius

    return distance


def euclidean_distance_v2(lat1, lon1, lat2, lon2):
    # Convert latitude and longitude from degrees to radians
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)

    # Earth's radius in meters
    R = 6371000  # Radius of the Earth in meters

    # Calculate differences in coordinates
    delta_lat = lat2_rad - lat1_rad
    delta_lon = lon2_rad - lon1_rad

    # Calculate Euclidean distance using the formula:
    # d = sqrt((delta_lat * R)^2 + (delta_lon * R * cos((lat1_rad + lat2_rad) / 2))^2)
    distance = math.sqrt((delta_lat * R)**2 + (delta_lon * R * math.cos((lat1_rad + lat2_rad) / 2))**2)

    return distance


def calculate_euclidean_distance(coord1, coord2):
    # Radius of the Earth in meters
    R = 6371000

    # Extract latitude and longitude from coordinates
    lat1_deg, lon1_deg = coord1
    lat2_deg, lon2_deg = coord2

    # Convert latitude and longitude from degrees to radians
    lat1_rad = math.radians(lat1_deg)
    lon1_rad = math.radians(lon1_deg)
    lat2_rad = math.radians(lat2_deg)
    lon2_rad = math.radians(lon2_deg)

    # Convert to Cartesian coordinates
    x1 = R * math.cos(lat1_rad) * math.cos(lon1_rad)
    y1 = R * math.cos(lat1_rad) * math.sin(lon1_rad)
    x2 = R * math.cos(lat2_rad) * math.cos(lon2_rad)
    y2 = R * math.cos(lat2_rad) * math.sin(lon2_rad)

    # Calculate Euclidean distance
    distance = math.sqrt((x2 - x1)**2 + (y2 - y1)**2)

    return distance #in meters
    

def equirectangular_distance(coord1, coord2):
   # Radius of the Earth in kilometers
   R = 6371000

   lat1, lon1 = math.radians(coord1[0]), math.radians(coord1[1])
   lat2, lon2 = math.radians(coord2[0]), math.radians(coord2[1])

   # Equirectangular approximation
   x = (lon2 - lon1) * math.cos((lat1 + lat2) / 2)
   y = lat2 - lat1
   distance = math.sqrt(x**2 + y**2) * R

   return distance #in meters


db_config = {
    'host': '192.168.150.112',
    'user': 'pysys_local',
    'password': 'NaCAhztBgYZ3HwTkvHwwGVtJn5sVMFgg',
    'database': 'analysis_db'
}

connection = mysql.connector.connect(**db_config)
cursor = connection.cursor()


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
            

# def fetch_base_station_coordinates(base_name):
#     try:
#         connection = mysql.connector.connect(**db_config)
#         cursor = connection.cursor()

#         # Fetch base station coordinates based on base_name
#         query = "SELECT latitude, longitude FROM base_stations WHERE base_name = %s"
#         cursor.execute(query, (base_name,))
#         row = cursor.fetchone()

#         if not row:
#             return None

#         return row[0], row[1]

#     except mysql.connector.Error as error:
#         print(f"Error fetching base station coordinates for {base_name}: {error}")
#         return None

#     finally:
#         if 'connection' in locals() and connection.is_connected():
#             cursor.close()
#             connection.close()
            

# def check_threshold_and_alert():
#     # Fetch all GNSS table names in the database
#     gnss_table_names = fetch_gnss_table_names()

#     for table_name in gnss_table_names:
#         timestamp, _, lat, lon = fetch_latest_gps_data(table_name)  # Ignore rover_name from fetch_latest_gps_data

#         if not (timestamp and lat and lon):
#             continue  # Skip if no valid GPS data for this table

#         # Extract rover name from table_name (assuming format is 'gnss_rovername')
#         rover_name = table_name.replace('gnss_', '', 1)

#         # Fetch base station coordinates based on rover_name
#         base_name_pattern = re.compile(r'^Base\d+$')
#         if not base_name_pattern.match(rover_name):
#             continue  # Skip if rover name doesn't match expected pattern

#         base_lat, base_lon = fetch_base_station_coordinates(rover_name)

#         if not (base_lat and base_lon):
#             continue  # Base station coordinates not found

#         rover_coords = (lat, lon)
#         base_coords = (base_lat, base_lon)
#         displacement_km = calculate_displacement(rover_coords, base_coords) #funtion to use -> calculate_euclidean_distance

#         if displacement_km >= threshold_km:
#             print(f"Threshold ({threshold_km} km) exceeded for {rover_name}! Displacement: {displacement_km} km")
#             # Code to trigger alert (e.g., send email, push notification, etc.)

# if __name__ == "__main__":
#     check_threshold_and_alert()


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

def check_threshold_and_alert():
    # Fetch all GNSS table names in the database
    gnss_table_names = fetch_gnss_table_names()

    for table_name in gnss_table_names:
        timestamp, _, lat, lon = fetch_latest_gps_data(table_name)  # Ignore rover_name from fetch_latest_gps_data

        if not (timestamp and lat and lon):
            continue  # Skip if no valid GPS data for this table

        # Extract rover name from table_name (assuming format is 'gnss_rovername')
        rover_name = table_name.replace('gnss_', '', 1)

        # Fetch base name for the rover
        base_name = fetch_base_name_for_rover(rover_name)

        if not base_name:
            continue  # Base name not found for this rover

        # Fetch base coordinates using the base name
        base_lat, base_lon = fetch_base_coordinates(base_name)

        if not (base_lat and base_lon):
            continue  # Base station coordinates not found

        rover_coords = (lat, lon)
        base_coords = (base_lat, base_lon)
        displacement_km = calculate_displacement(rover_coords, base_coords)

        if displacement_km >= threshold_km:
            print(f"Threshold ({threshold_km} km) exceeded for {rover_name}! Displacement: {displacement_km} km")
            # Code to trigger alert (e.g., send email, push notification, etc.)
