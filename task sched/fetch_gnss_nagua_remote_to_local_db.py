import mysql.connector

# Function to get the latest data from the remote database
def fetch_latest_data_from_remote():
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
        # dyna_db_cursor.execute("SET NAMES 'utf8mb4'")
        
        # Fetch the latest data
        query = "SELECT ts, fix_type, latitude, longitude, hacc, vacc, msl, sat_num, temp, volt FROM gnss_nagua WHERE ts > NOW() - INTERVAL 4 hour"
        dyna_db_cursor.execute(query)
        latest_data = dyna_db_cursor.fetchall()
        
        dyna_db_cursor.close()
        dyna_db.close()
        
        return latest_data
    
    except Exception as e:
        print(f"Error fetching data from remote database: {e}")
        return None

# Function to check for duplicates and insert new data into the local database
def update_local_database(data):
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
        # local_cursor.execute("SET NAMES 'utf8mb4'")
        
        # Insert data into the local database if it doesn't already exist
        for record in data:
            # Assuming the first column is the unique identifier
            ts = record[0]
            
            # Check if the record already exists
            check_query = "SELECT 1 FROM gnss_nagua WHERE ts = %s"
            local_cursor.execute(check_query, (ts,))
            if local_cursor.fetchone() is None:
                # Insert the new record
                insert_query = """
                                INSERT INTO gnss_nagua (ts, fix_type, latitude, longitude, hacc, vacc, msl, sat_num, temp, volt) 
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                """
                local_cursor.execute(insert_query, record)
        
        # Commit the transaction
        local_conn.commit()
        local_cursor.close()
        local_conn.close()
        
    except Exception as e:
        print(f"Error updating local database: {e}")

# Main function
def main():
    latest_data = fetch_latest_data_from_remote()
    if latest_data:
        update_local_database(latest_data)

if __name__ == "__main__":
    main()
