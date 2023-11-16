#!/bin/bash

# Execute the data pipeline
python3 /project/pipeline.py

employee_data_file="./main/data/employees_data.sqlite"
RD_Expenditure_data_file="./main/data/R&D_Expenditure.sqlite"

# Check if the Employee output file exists
if [ -f "$employee_data_file" ]; then
    echo "Yes! Output file $employee_data_file is available."
else
    echo "No! Output file $employee_data_file is not available."
fi

# Check if the R&D Expenditure output file exists
if [ -f "$RD_Expenditure_data_file" ]; then
    echo "Yes! Output file $RD_Expenditure_data_file is available."
else
    echo "No! Output file $RD_Expenditure_data_file is not available."
fi