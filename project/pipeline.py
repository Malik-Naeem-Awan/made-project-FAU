import sqlite3
import pandas as pd
import cbsodata
import time


# Function to fetch data with retries
def fetch_data_with_retry(table, filters, max_attempts=5):
    attempts = 0
    while attempts < max_attempts:
        try:
            # Downloading entire dataset (can take up to 30s)
            data = pd.DataFrame(cbsodata.get_data(table, filters=filters))
            return data
        except Exception as e:
            attempts += 1
            print(f"Attempt {attempts} failed. Error: {e}")
            time.sleep(1)  # Wait for 1 second before the next attempt
    return pd.DataFrame()  # Return an empty DataFrame if max attempts are reached


# Read data of Research and development by company sizes in netherlands from online source:
# Define filter conditions
filter_condition = (
    "EmployeeWithWithoutRegistration eq 'T001391' and "
    "EmployeeCharacteristics eq 'T001097' and "
    "SectorBranchesSIC2008 eq 'T001081' and "
    "JobCharacteristics eq 'T001025' and "
    "MigrationBackgroundNationality eq 'T001040' and "
    "(Periods eq '2013JJ00' or Periods eq '2014JJ00' or Periods eq '2015JJ00' or Periods eq '2016JJ00' or "
    "Periods eq '2017JJ00')"
)

# Fetch data with retry mechanism
df1 = fetch_data_with_retry('84060ENG', filters=filter_condition)

# Check if DataFrame is empty after retries
if df1.empty:
    print("Failed to fetch data after multiple attempts.")
else:
    # Clean and preprocess the Employees data
    df1 = df1.drop(columns=['ID', 'SectorBranchesSIC2008', 'JobCharacteristics', 'EmployeeWithWithoutRegistration',
                            'MigrationBackgroundNationality', 'EmployeeCharacteristics'])

    # Rename the Columns of Employees data
    df1.columns = ['Year', 'Number of employees from abroad']

    # Number of employees from abroad are given in divisible by 1000
    # it means 689 means 689000 So modifying the actual values
    df1['Number of employees from abroad'] = df1['Number of employees from abroad'] * 1000

    # Create SQLite connection for Employees data
    conn = sqlite3.connect('../data/employees_data.sqlite')

    # Insert data into SQLite database
    df1.to_sql('employees', conn, if_exists='replace', index=False)

    # Commit changes and close connection
    conn.commit()
    conn.close()

    print("Employee data has been successfully loaded into the SQLite database.")

# Read data of Research and development by company sizes in netherlands from online source:
# Define filter conditions
filter_condition = (
    "SectorBranchesSIC2008 eq 'T001081' and "
    "CompanySize eq 'T001098' and "
    "(Periods eq '2013JJ00' or Periods eq '2014JJ00' or Periods eq '2015JJ00' or Periods eq '2016JJ00' or "
    "Periods eq '2017JJ00')"
)

# Fetch R&D Expenditure data with retry mechanism
df2 = fetch_data_with_retry('84985ENG', filters=filter_condition)

# Check if DataFrame is empty after retries
if df2.empty:
    print("Failed to fetch R&D Expenditure data after multiple attempts.")
else:
    # Clean and preprocess the R&D Expenditure data
    df2 = df2.drop(
        columns=['ID', 'SectorBranchesSIC2008', 'YearsOfWork_2', 'EnterprisesWithInHouseRDActivities_4', 'CompanySize'])

    # Adding column names for R&D Expenditure data
    df2.columns = ['Year', 'Total R&D Employees', 'Total R&D Expenditure']

    # Create SQLite connection for Employees data
    conn = sqlite3.connect('../data/R&D_Expenditure.sqlite')

    # Insert data into SQLite database
    df2.to_sql('R&D_Expenditure', conn, if_exists='replace', index=False)

    # Commit changes and close connection
    conn.commit()
    conn.close()

    print("R&D Expenditure data has been successfully loaded into the SQLite database.")