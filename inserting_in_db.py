# Importing required libraries
import pymongo
import pandas as pd

# MongoDB connection
client = pymongo.MongoClient('localhost', 27017)  # Replace 'localhost' and '27017' with your MongoDB connection details
db = client['store_data']  # Replace 'store_data' with your preferred database name

# Schema for StoreActivity collection
store_activity_schema = {
    'store_id': int,
    'timestamp_utc': int,
    'status': str
}

# Creating the StoreActivity collection
store_activity_collection = db['store_activity']


# Schema for StoreBusinessHours collection
store_business_hours_schema = {
    'store_id': int,
    'dayOfWeek': int,  # 0=Monday, 6=Sunday
    'start_time_local': str,
    'end_time_local': str
}

# Creating the StoreBusinessHours collection
store_business_hours_collection = db['store_business_hours']

# Schema for StoreTimezone collection
store_timezone_schema = {
    'store_id': int,
    'timezone_str': str
}

# Creating the StoreTimezone collection
store_timezone_collection = db['store_timezone']

# Load data from CSV files
df_store_activity = pd.read_csv('https://drive.google.com/file/d/1UIx1hVJ7qt_6oQoGZgb8B3P2vd1FD025/view?usp=sharing')
df_store_business_hours = pd.read_csv('https://drive.google.com/file/d/1va1X3ydSh-0Rt1hsy2QSnHRA4w57PcXg/view?usp=sharing')
df_store_timezone = pd.read_csv('https://drive.google.com/file/d/101P9quxHoMZMZCVWQ5o-shonk2lgK1-o/view?usp=sharing')

# Insert data into MongoDB collections
store_activity_collection.insert_many(df_store_activity.to_dict('records'))
store_business_hours_collection.insert_many(df_store_business_hours.to_dict('records'))
store_timezone_collection.insert_many(df_store_timezone.to_dict('records'))
