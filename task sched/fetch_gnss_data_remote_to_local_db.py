import mysql.connector

# Function to get the list of GNSS tables from the remote database
def get_gnss_tables():
    try:
        # Connect to the remote database
        dyna_db = mysql.connector.connect(
                    host="192.168.150.112",
                    database="analysis_db",
                    user="pysys_local",
                    password="NaCAhztBgYZ3HwTkvHwwGVtJn5sVMFgg",
                    charset="utf8mb4"  # Explicitly setting charset
                    )
        dyna_db_cursor = dyna_db.cursor()
        
        # Get the list of GNSS tables
        query = "SHOW TABLES LIKE 'gnss_%'"
        dyna_db_cursor.execute(query)
        gnss_tables = [row[0] for row in dyna_db_cursor.fetchall()]
        
        dyna_db_cursor.close()
        dyna_db.close()
        
        return gnss_tables
    
    except Exception as e:
        print(f"Error fetching GNSS tables from remote database: {e}")
        return []

# Function to get the latest data from the remote database
def fetch_latest_data_from_remote(table_name):
    try:
        # Connect to the remote database
        dyna_db = mysql.connector.connect(
                    host="192.168.150.112",
                    database="analysis_db",
                    user="pysys_local",
                    password="NaCAhztBgYZ3HwTkvHwwGVtJn5sVMFgg",
                    charset="utf8mb4"  # Explicitly setting charset
                    )
        dyna_db_cursor = dyna_db.cursor()
        
        # Fetch the latest data
        query = f"SELECT ts, fix_type, latitude, longitude, hacc, vacc, msl, sat_num, temp, volt FROM {table_name} WHERE ts > NOW() - INTERVAL 4 hour"
        dyna_db_cursor.execute(query)
        latest_data = dyna_db_cursor.fetchall()
        
        dyna_db_cursor.close()
        dyna_db.close()
        
        return latest_data
    
    except Exception as e:
        print(f"Error fetching data from remote database ({table_name}): {e}")
        return None

# Function to check for duplicates and insert new data into the local database
def update_local_database(data, table_name):
    try:
        # Connect to the local database
        local_conn = mysql.connector.connect(
                            host="localhost",
                            database="new_schema",
                            user="root",
                            password="admin123",
                            charset="utf8mb4"  # Explicitly setting charset
                        )
        local_cursor = local_conn.cursor()
        
        # Insert data into the local database if it doesn't already exist
        for record in data:
            # Assuming the first column is the unique identifier
            ts = record[0]
            
            # Check if the record already exists
            check_query = f"SELECT 1 FROM {table_name} WHERE ts = %s"
            local_cursor.execute(check_query, (ts,))
            if local_cursor.fetchone() is None:
                # Insert the new record
                insert_query = f"""
                                INSERT INTO {table_name} (ts, fix_type, latitude, longitude, hacc, vacc, msl, sat_num, temp, volt) 
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                """
                local_cursor.execute(insert_query, record)
        
        # Commit the transaction
        local_conn.commit()
        local_cursor.close()
        local_conn.close()
        
    except Exception as e:
        print(f"Error updating local database ({table_name}): {e}")

# # Main function
# def main():
#     gnss_tables = get_gnss_tables()
#     if gnss_tables:
#         for table_name in gnss_tables:
#             latest_data = fetch_latest_data_from_remote(table_name)
#             if latest_data:
#                 update_local_database(latest_data, table_name)
#     else:
#         print("No GNSS tables found in the remote database.")

# Main function
def main():
    table_names = ["gnss_nagua", "gnss_sinua"]  # Add your table names here
    for table_name in table_names:
        latest_data = fetch_latest_data_from_remote(table_name)
        if latest_data:
            update_local_database(latest_data, table_name)

if __name__ == "__main__":
    main()
