import csv
import zipfile
import pandas as pd
from sqlalchemy import create_engine, Text, Float, Integer
import urllib.request


# Downloading the GTFS data from given download URL
download_url = 'https://gtfs.rhoenenergie-bus.de/GTFS.zip'
urllib.request.urlretrieve(download_url, 'GTFS.zip')


# Extracting the Zip file
with zipfile.ZipFile('GTFS.zip', 'r') as zip_file_reference:
    zip_file_reference.extractall('GTFS')


# Filtering the data and validate also
filtered_and_validated_data = []
with open('GTFS/stops.txt', 'r', encoding='utf-8-sig') as csv_file:
    csv_file_read = csv.DictReader(csv_file)

    for data_row in csv_file_read:
        name_of_stop = data_row['stop_name']
        if (
                data_row['zone_id'] == '2001'
                and -90 <= float(data_row['stop_lat']) <= 90
                and -90 <= float(data_row['stop_lon']) <= 90
        ):
            filtered_and_validated_data.append({
                'stop_id': data_row['stop_id'],
                'name_of_stop': data_row['stop_name'],
                'stop_lat': float(data_row['stop_lat']),
                'stop_lon': float(data_row['stop_lon']),
                'zone_id': int(data_row['zone_id'])
            })


# Converting the filtered_and_validated_data to a DataFrame
filtered_and_validated_dataframe = pd.DataFrame(filtered_and_validated_data)


# Define SQLite types for each column
sqlite_data_types = {
    'stop_id': Integer(),
    'name_of_stop': Text(),
    'stop_lat': Float(),
    'stop_lon': Float(),
    'zone_id': Integer()
}


sqlite_engine = create_engine('sqlite:///gtfs.sqlite')


filtered_and_validated_dataframe.to_sql('stops', sqlite_engine, if_exists='replace', index=False, dtype=sqlite_data_types)


# Closing the database connection after saving the data
sqlite_engine.dispose()

print("SQLite database created!")
