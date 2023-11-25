#!/bin/bash

# Change directory to the project directory
cd /home/runner/work/made-project-FAU/main/project

# Execute tests using pytest
pytest test_pipeline.py

# Execute the data pipeline
python3 pipeline.py

# Define the file paths for output files
db_file_employees='../data/employees_data.sqlite'
db_file_rd_expenditure='../data/R&D_Expenditure.sqlite'

# Check if the output files exist
if [ -f "$db_file_employees" ]; then
    echo "Yes! Output file $db_file_employees is available."
else
    echo "No! Output file $db_file_employees is not available."
fi

if [ -f "$db_file_rd_expenditure" ]; then
    echo "Yes! Output file $db_file_rd_expenditure is available."
else
    echo "No! Output file $db_file_rd_expenditure is not available."
fi
