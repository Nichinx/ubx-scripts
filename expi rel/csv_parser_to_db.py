# -*- coding: utf-8 -*-
"""
Created on Mon Oct 14 05:42:38 2024

@author: nichm
"""

import numpy as np
import pandas as pd
from datetime import datetime as dt
from sqlalchemy import create_engine
from sqlalchemy.engine import Connection

db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'admin123',
    'database': 'new_schema_4'
}

def parse_line(line):
    split_line = line.strip().split('*')
    ts = split_line[1]
    

    sms = split_line[0]
    split_data = sms.split(':')
    logger_name = split_data[0]
    
    data_part = split_data[1]
    ublox_data = data_part.split(',')

    # Create a DataFrame from the parsed data
    trans_ublox_data = pd.DataFrame([ublox_data], columns=["fix_type", "latitude", "longitude", "hacc", "vacc", "msl", "sat_num", "temp", "volt"])
    trans_ublox_data["ts"] = ts

    return logger_name, trans_ublox_data

# Path to your text file
file_path = "C:\\Users\\nichm\\Downloads\\UP_muhon_test.csv"  # Update this to your actual file path

dataframes = []
logger_names = set()
with open(file_path, 'r') as file:
    for line in file:
        logger_name, parsed_data = parse_line(line)
        if parsed_data is not None:
            dataframes.append(parsed_data)
            logger_names.add(logger_name)

# Concatenate all DataFrames into a single DataFrame
if dataframes:
    final_df = pd.concat(dataframes, ignore_index=True)
    print("Final DataFrame:")
    print(final_df)
    
    for logger_name in logger_names:
        table_name = f"gnss_{logger_name.lower()}" 
    
        connection_string = f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config.get('port', 3306)}/{db_config['database']}"
        engine = create_engine(connection_string)
        final_df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
        print(f"Data stored in the table: {table_name}")
else:
    print("No valid data found to store.")