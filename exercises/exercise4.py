import os
import time
import pandas as pd
from sqlalchemy import BIGINT, FLOAT, TEXT
from pathlib import Path
from typing import Callable, Any
import urllib.request
import zipfile
import shutil
# Code to Download and extract the zip file starts here


def fetch_and_unzip(url: str, max_tries: int = 5, wait_for_seconds_before_retrying: float = 5) -> str:
    
    data_file_name = Path(url).stem
    path_to_extract = os.path.join(os.curdir, data_file_name)
    name_of_zip_file = data_file_name + '.zip'
    
    # Extracting zip file with multiple retries if needed
    for i in range(1, max_tries + 1):
        
        try:
            urllib.request.urlretrieve(url, name_of_zip_file)
            break
            
        except:
            print(f'Couldn\'t downloading the zip file from given url! (Try {i}/{max_tries})')
            if i < max_tries: time.sleep(wait_for_seconds_before_retrying)
            
    # Checking if the file was downloaded successfully
    if not os.path.exists(name_of_zip_file):
        raise FileNotFoundError(f'Failed to download the zip file from the given url {url}')
    # Extract and delete zip file
    with zipfile.ZipFile(name_of_zip_file, 'r') as zip_ref:
        zip_ref.extractall(path_to_extract)
    os.remove(name_of_zip_file)
    # Return path to extracted data
    return path_to_extract
# Code to Download and extract the zip file ends here
# Code to validate the data starts here


def data_validation(df: pd.DataFrame, column: str, constraint: Callable[[Any], bool]) -> pd.DataFrame:
    df = df.loc[df[column].apply(constraint)]
    return df
# Code to validate the data ends here
# Code to convert degree  celsius to degree fahrenheit starts here


def convert_degree_celsius_to_fahrenheit(temp_degrees: float) -> float:
    return (temp_degrees * 9 / 5) + 32
# Code to convert degree  celsius to degree fahrenheit ends here


# Main code starts here
if __name__ == '__main__':
    zip_file_url = 'https://www.mowesta.com/data/measure/mowesta-dataset-20221107.zip'
    data_filename = 'data.csv'

    # Download and unzip data
    data_file_path = fetch_and_unzip(zip_file_url)

    # Read and reshape data
    df = pd.read_csv(os.path.join(data_file_path, data_filename),
                     sep=';',
                     index_col=False,
                     usecols=['Geraet', 'Hersteller', 'Model', 'Monat', 'Temperatur in °C (DWD)',
                              'Batterietemperatur in °C', 'Geraet aktiv'],
                     decimal=',')

    # Renaming the columns accordingly
    df.rename(columns={
        'Temperatur in °C (DWD)': 'Temperatur',
        'Batterietemperatur in °C': 'Batterietemperatur'
    }, inplace=True)

    # Transforming the data accordingly
    df['Temperatur'] = convert_degree_celsius_to_fahrenheit(df['Temperatur'])
    df['Batterietemperatur'] = convert_degree_celsius_to_fahrenheit(df['Batterietemperatur'])

    # Validating the data accordingly
    df = data_validation(df, 'Geraet', lambda x: x > 0)
    df = data_validation(df, 'Monat', lambda x: x in range(1, 13))
    df = data_validation(df, 'Temperatur', lambda x: -459.67 < x < 212)
    df = data_validation(df, 'Batterietemperatur', lambda x: -459.67 < x < 212)
    df = data_validation(df, 'Geraet aktiv', lambda x: x in ['Ja', 'Nein'])

    # Writing the validated data into an SQLite database accordingly
    table = 'temperatures'
    database_name = 'temperatures.sqlite'
    df.to_sql(table, f'sqlite:///{database_name}', if_exists='replace', index=False, dtype={
        'Geraet': BIGINT,
        'Hersteller': TEXT,
        'Model': TEXT,
        'Monat': BIGINT,
        'Temperatur': FLOAT,
        'Batterietemperatur': FLOAT,
        'Geraet aktiv': TEXT
    })

    # Deleting the data which was downloaded earlier accordingly
    shutil.rmtree(data_file_path)

    print('Complete run of Datapipeline has finished successfully')
    print(f'The Downloaded data is stored in the table named as "{table}" in the SQLite database named "{database_name}"')
# Main code ends here
