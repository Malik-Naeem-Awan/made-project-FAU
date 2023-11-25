import sqlite3
import time
import pandas as pd
import cbsodata

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


def get_datasource_1() -> pd.DataFrame:
    print(" - Loading Datasource 1")

    filter_condition = (
        "EmployeeWithWithoutRegistration eq 'T001391' and "
        "EmployeeCharacteristics eq 'T001097' and "
        "SectorBranchesSIC2008 eq 'T001081' and "
        "JobCharacteristics eq 'T001025' and "
        "MigrationBackgroundNationality eq 'T001040' and "
        "(Periods eq '2013JJ00' or Periods eq '2014JJ00' or Periods eq '2015JJ00' or Periods eq '2016JJ00' or "
        "Periods eq '2017JJ00')"
    )

    df1 = fetch_data_with_retry('84060ENG', filters=filter_condition)

    if df1.empty:
        print("Failed to fetch data after multiple attempts.")
    else:
        df1 = df1.drop(columns=['ID', 'SectorBranchesSIC2008', 'JobCharacteristics', 'EmployeeWithWithoutRegistration',
                                'MigrationBackgroundNationality', 'EmployeeCharacteristics'])
        df1.columns = ['Year', 'Number of employees from abroad']
        df1['Number of employees from abroad'] = df1['Number of employees from abroad'] * 1000
    return df1


def get_datasource_2() -> pd.DataFrame:
    print(" - Loading Datasource 2")

    filter_condition = (
        "SectorBranchesSIC2008 eq 'T001081' and "
        "CompanySize eq 'T001098' and "
        "(Periods eq '2013JJ00' or Periods eq '2014JJ00' or Periods eq '2015JJ00' or Periods eq '2016JJ00' or "
        "Periods eq '2017JJ00')"
    )

    df2 = fetch_data_with_retry('84985ENG', filters=filter_condition)

    if df2.empty:
        print("Failed to fetch R&D Expenditure data after multiple attempts.")
    else:
        df2 = df2.drop(columns=['ID', 'SectorBranchesSIC2008', 'YearsOfWork_2', 'EnterprisesWithInHouseRDActivities_4',
                                'CompanySize'])
        df2.columns = ['Year', 'Total R&D Employees', 'Total R&D Expenditure']
    return df2


def store_dataframe(df: pd.DataFrame, table: str, db_path: str):
    print(f" - Storing Data into table '{table}' of database '{db_path}'")
    conn = sqlite3.connect(db_path)
    df.to_sql(table, conn, if_exists='replace', index=False)
    conn.commit()
    conn.close()


def main():
    db_path_employees = '../data/employees_data.sqlite'
    db_path_rd_expenditure = '../data/R&D_Expenditure.sqlite'

    # fetch and store data for Employees from abroad
    df1 = get_datasource_1()
    store_dataframe(df1, 'employees', db_path_employees)

    # fetch and store data for R&D Expenditure
    df2 = get_datasource_2()
    store_dataframe(df2, 'R&D_Expenditure', db_path_rd_expenditure)

if __name__ == "__main__":
    main()
    