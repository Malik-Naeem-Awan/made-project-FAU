#!/bin/bash

pytest test_datapipeline.py   

# Execute the data pipeline
python3 pipeLine.py

db_file_employees = './data/employees_data.sqlite'
db_file_rd_expenditure = './data/R&D_Expenditure.sqlite'

# Check if the output file exists
if [ -f "$db_file_employees" ]; then
    echo "Yes! Output file $db_file_employees is available."
else
    echo "No! Output file $db_file_employees is not available."
fi

# Check if the output file exists
if [ -f "$db_file_rd_expenditure" ]; then
    echo "Yes! Output file $db_file_rd_expenditure is available."
else
    echo "No! Output file $db_file_rd_expenditure is not available."
fi
