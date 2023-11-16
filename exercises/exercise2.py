import re
import time
from typing import Callable, Any
import pandas as pd
import sqlalchemy


def extract_data_from_csv_to_df(url: str, max_tries: int = 5, sec_wait_before_retry: float = 5) -> pd.DataFrame:
    df = None
    for i in range(1, max_tries + 1):
        try:
            df = pd.read_csv(url, sep=';', decimal=',')
            break
        except:
            print(f'Unable to extract csv file from the given url! (Try {i}/{max_tries})')
            if i < max_tries: time.sleep(sec_wait_before_retry)
    if df is None:
        raise Exception(f'Error occurred while extracting csv from given url {url}')
    return df


if __name__ == '__main__':
    # Extracting CSV file from given file URL
    DATA_URL = 'https://download-data.deutschebahn.com/static/datasets/haltestellen/D_Bahnhof_2020_alle.CSV'

    # Extracting data from CSV to Data frame to be used in next process
    df = extract_data_from_csv_to_df(DATA_URL)

    # Removing column: Status
    df = df.drop(columns=['Status'])

    # Removing entries that do not have valid values
    df = df[
        (df['Verkehr'].isin(['FV', 'RV', 'nur DPN'])) &
        (df['Laenge'].between(-90, 90, inclusive='both')) &
        (df['Breite'].between(-90, 90, inclusive='both')) &
        (df['IFOPT'].str.match(r'^[a-zA-Z]{2}:\d+:\d+(\:\d+)?$')) &
        (df.notnull().all(axis=1))
        ]

    # Loading the dataframe into sqlite database trainstops.sqlite, with matching datatypes
    df.to_sql('trainstops', 'sqlite:///trainstops.sqlite', if_exists='replace', index=False, dtype={
        "EVA_NR": sqlalchemy.BIGINT,
        "DS100": sqlalchemy.TEXT,
        "IFOPT": sqlalchemy.TEXT,
        "NAME": sqlalchemy.TEXT,
        "Verkehr": sqlalchemy.TEXT,
        "Laenge": sqlalchemy.FLOAT,
        "Breite": sqlalchemy.FLOAT,
        "Betreiber_Name": sqlalchemy.TEXT,
        "Betreiber_Nr": sqlalchemy.BIGINT
    })
