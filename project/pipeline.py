# Pipeline for importing and storing data from online sources to sqlite databases.

import sqlite3
import time
import pandas as pd
import cbsodata


# function to fetch data with retries and exception handling
def fetch_data_with_retry(table, filters, max_attempts=5):
    attempts = 0
    while attempts < max_attempts:
        try:
            data = pd.DataFrame(cbsodata.get_data(table, filters=filters))
            return data
        except Exception as e:
            attempts += 1
            print(f"Attempt {attempts} failed. Error: {e}")
            time.sleep(1)
    return pd.DataFrame()


# function to get first datasource of employees from abroad
def get_datasource_1() -> pd.DataFrame:
    print(" - Loading Datasource 1")

    EmployeeWithWithoutRegistration = "EmployeeWithWithoutRegistration eq 'T001391' and "
    EmployeeCharacteristics = "EmployeeCharacteristics eq 'T001097' and "
    SectorBranchesSIC2008 = "SectorBranchesSIC2008 eq 'T001081' and "
    JobCharacteristics = "JobCharacteristics eq 'T001025' and "
    MigrationBackgroundNationality = "MigrationBackgroundNationality eq 'T001040' and "
    Periods = "(Periods eq '2013JJ00' or Periods eq '2014JJ00' or "
    Periods_2015_16 = "Periods eq '2015JJ00' or Periods eq '2016JJ00' or "
    Periods_2017 = "Periods eq '2017JJ00')"


    filter_condition = (
        EmployeeWithWithoutRegistration+
        EmployeeCharacteristics+
        SectorBranchesSIC2008+
        JobCharacteristics+
        MigrationBackgroundNationality+
        Periods+
        Periods_2015_16+
        Periods_2017
    )

    df = fetch_data_with_retry('84060ENG', filters=filter_condition)

    if df.empty:
        print("Failed to fetch data after multiple attempts.")
    else:
        df = df.drop(columns=['ID', 'SectorBranchesSIC2008', 'JobCharacteristics', 'EmployeeWithWithoutRegistration',
                                'MigrationBackgroundNationality', 'EmployeeCharacteristics'])
        df.columns = ['Year', 'Number of employees from abroad']
        df['Number of employees from abroad'] = df['Number of employees from abroad'] * 1000
    return df


# function to get second datasource of R&D expenditure
def get_datasource_2() -> pd.DataFrame:
    print(" - Loading Datasource 2")

    SectorBranchesSIC2008 = "SectorBranchesSIC2008 eq 'T001081' and "
    CompanySize = "CompanySize eq 'T001098' and "
    Periods_201314 = "(Periods eq '2013JJ00' or Periods eq '2014JJ00' "
    Periods_201516 = "or Periods eq '2015JJ00' or Periods eq '2016JJ00' or "
    Periods_2017 = "Periods eq '2017JJ00')"


    filter_condition = (
        SectorBranchesSIC2008+
        CompanySize+
        Periods_201314+
        Periods_201516+
        Periods_2017
    )

    df = fetch_data_with_retry('84985ENG', filters=filter_condition)

    if df.empty:
        print("Failed to fetch R&D Expenditure data after multiple attempts.")
    else:
        df = df.drop(columns=['ID', 'SectorBranchesSIC2008', 'YearsOfWork_2', 'EnterprisesWithInHouseRDActivities_4',
                                'CompanySize'])
        df.columns = ['Year', 'Total R&D Employees', 'Total R&D Expenditure']
    return df


# function to store dataframes or datasources into sqlite databases
def store_dataframe(df: pd.DataFrame, table: str, db_path: str):
    print(f" - Storing Data into table '{table}' of database '{db_path}'")
    conn = sqlite3.connect(db_path)
    df.to_sql(table, conn, if_exists='replace', index=False)
    conn.commit()
    conn.close()


# function that executes main pipeline
def main():
    db_path_employees = '../data/employees_data.sqlite'
    db_path_rd_expenditure = '../data/R&D_Expenditure.sqlite'

    # fetch and store data for Employees from abroad
    df1 = get_datasource_1()
    store_dataframe(df1, 'employees', db_path_employees)

    # fetch and store data for R&D Expenditure
    df2 = get_datasource_2()
    store_dataframe(df2, 'R&D_Expenditure', db_path_rd_expenditure)


# function to call main pipeline execution function
if __name__ == "__main__":
    main()
    