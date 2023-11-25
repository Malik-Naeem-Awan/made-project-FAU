"""
Pipeline to load from online source and store the data in sqlite database.
"""

import sqlite3
import time
import pandas as pd
import cbsodata


def fetch_data_with_retry(table, filters, max_attempts=5):
    """
    Fetches data from CBS Open Data API with retry mechanism.
    """
    attempts = 0
    while attempts < max_attempts:
        try:
            data = pd.DataFrame(cbsodata.get_data(table, filters=filters))
            return data
        except pd.errors.EmptyDataError as empty_data_error:
            # Handle EmptyDataError (exceptions related to empty DataFrame)
            print(f"Empty DataFrame error: {empty_data_error}")
            attempts += 1
            print(f"Attempt {attempts} failed. Error: {empty_data_error}")
            time.sleep(1)
    return pd.DataFrame()


def get_datasource_1() -> pd.DataFrame:
    """
    Loads Datasource 1.
    """
    print(" - Loading Datasource 1")

    filter_condition = (
        "EmployeeWithWithoutRegistration eq 'T001391'"
        " and "
        "EmployeeCharacteristics eq 'T001097'"
        " and "
        "SectorBranchesSIC2008 eq 'T001081'"
        " and "
        "JobCharacteristics eq 'T001025'"
        " and "
        "MigrationBackgroundNationality eq 'T001040'"
        " and "
        "(Periods eq '2013JJ00'"
        " or "
        "Periods eq '2014JJ00' "
        "or "
        "Periods eq '2015JJ00' "
        "or "
        "Periods eq '2016JJ00' "
        "or "
        "Periods eq '2017JJ00')"
    )
    df_fetched = fetch_data_with_retry('84060ENG', filters=filter_condition)

    if df_fetched.empty:
        print("Failed to fetch data after multiple attempts.")
        df_processed = None
    else:
        df_processed = df_fetched.drop(columns=['ID',
                                                'SectorBranchesSIC2008',
                                                'JobCharacteristics',
                                                'EmployeeWithWithoutRegistration',
                                                'MigrationBackgroundNationality',
                                                'EmployeeCharacteristics'])
        if not df_processed.empty:
            df_processed.columns = ['Year', 'Number of employees from abroad']
            if 'Number of employees from abroad' in df_processed.columns:
                df_processed['Number of employees from abroad'] *= 1000
    return df_processed


def get_datasource_2() -> pd.DataFrame:
    """
    Loads Datasource 2.
    """
    print(" - Loading Datasource 2")

    filter_condition = (
        "SectorBranchesSIC2008 eq 'T001081'"
        " and "
        "CompanySize eq 'T001098'"
        " and "
        "(Periods eq '2013JJ00'"
        " or "
        "Periods eq '2014JJ00' "
        "or "
        "Periods eq '2015JJ00'"
        " or "
        "Periods eq '2016JJ00' "
        "or "
        "Periods eq '2017JJ00')"
    )

    df = fetch_data_with_retry('84985ENG', filters=filter_condition)

    if df.empty:
        print("Failed to fetch R&D Expenditure data "
              "after multiple attempts.")
    else:
        df = df.drop(columns=['ID',
                                'SectorBranchesSIC2008',
                                'YearsOfWork_2',
                                'EnterprisesWithInHouseRDActivities_4',
                                'CompanySize'])
        df.columns = ['Year', 'Total R&D Employees', 'Total R&D Expenditure']
    return df


def store_dataframe(df: pd.DataFrame, table: str, db_path: str):
    """
    Stores DataFrame into SQLite database.
    """
    print(f" - Storing Data into table "
          f"'{table}' of database '{db_path}'")
    conn = sqlite3.connect(db_path)
    df.to_sql(table, conn, if_exists='replace', index=False)
    conn.commit()
    conn.close()


def main():
    """
    Main function to fetch and store data.
    """
    db_path_employees = '../data' \
                        '/employees_data.sqlite'
    db_path_rd_expenditure = '../data' \
                             '/R&D_Expenditure.sqlite'

    # fetch and store data for Employees from abroad
    df1 = get_datasource_1()
    store_dataframe(df1, 'employees',
                    db_path_employees)

    # fetch and store data for R&D Expenditure
    df2 = get_datasource_2()
    store_dataframe(df2, 'R&D_Expenditure',
                    db_path_rd_expenditure)


if __name__ == "__main__":
    main()
