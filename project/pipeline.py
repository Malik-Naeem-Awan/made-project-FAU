import os
import pandas as pd
import sqlite3

# Read data of Employees from Abroad from CSV file:
df1 = pd.read_csv('./Employees_from_abroad__2010_2017_overall.csv', on_bad_lines='skip',index_col=0)

# Clean and preprocess the Employees data
df1 = df1.drop(columns=['Sector/Branches (SIC 2008)','Job characteristics', 'Migration background/Nationality','Employee characteristics'])

# Rename the Columns of Employees data
df1.columns = ['Year','employees from abroad']

# Number of employees from abroad are given in divisible by 1000
# it means 689 means 689000 So modifying the actual values
df1['employees from abroad'] = df1['employees from abroad'] * 1000

# Read data of Research and development by company sizes in netherlands from CSV file:
df2 = pd.read_csv('./R_D__company_size__branch_overall.csv', on_bad_lines='skip',index_col=0)

# Clean and preprocess the R&D Expenditure data
df2 = df2.drop(columns=['R&D personnel/Years of work (number)','Sector/branches (SIC2008)', 'Enterprises with in-house R&D activities (number)'])

# Adding column names for R&D Expenditure data
df2.columns = ['Year','Total R&D Employees','Total R&D Expenditure']


# Create SQLite connection for Employees data
conn = sqlite3.connect('./data/employees_data.sqlite')

# Insert data into SQLite database
df1.to_sql('employees', conn, if_exists='replace', index=False)

# Commit changes and close connection
conn.commit()
conn.close()

print("Employee data has been successfully loaded into the SQLite database.")

# Create SQLite connection for Employees data
conn = sqlite3.connect('./data/R&D_Expenditure.sqlite')

# Insert data into SQLite database
df2.to_sql('R&D_Expenditure', conn, if_exists='replace', index=False)

# Commit changes and close connection
conn.commit()
conn.close()

print("R&D Expenditure data has been successfully loaded into the SQLite database.")