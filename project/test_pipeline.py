"""
Module for testing the module datapipeline.py
"""
import os
import numpy as np
import pandas as pd
import pytest
from sqlalchemy import create_engine, inspect
import pipeline as pipe


@pytest.mark.dependency()
def test_get_datasources():
    """ Tests the methods to load the datasources """
    ds1 = pipe.get_datasource_1()
    assert isinstance(ds1, pd.DataFrame), "Data source 1 is not a Pandas DataFrame."
    print("Data source 1 is a Pandas DataFrame. Test Passed.")

    ds2 = pipe.get_datasource_2()
    assert isinstance(ds2, pd.DataFrame), "Data source 2 is not a Pandas DataFrame."
    print("Data source 2 is a Pandas DataFrame. Test Passed.")


@pytest.mark.dependency()
def test_store_dataframe():
    """ Tests the method to store a dataframe in a sqlite database """
    db_path_test = '../data/data_test.sqlite'
    df = pd.DataFrame(np.arange(12).reshape(3, 4), columns=["A", "B", "C", "D"])

    pipe.store_dataframe(df, "test", db_path_test)

    assert os.path.exists(db_path_test), f"Database file '{db_path_test}' does not exist."
    print(f"Database file '{db_path_test}' exists. Test Passed.")

    engine = create_engine(f"sqlite:///{db_path_test}")
    inspector = inspect(engine)
    tables = inspector.get_table_names()

    assert len(tables) == 1, f"Expected 1 table, found {len(tables)} tables."
    print(f"Number of tables in the database: {len(tables)}. Test Passed.")

    # Check if the 'test' table is in the list of table names
    assert "test" in tables, "Test table not found in the database."
    print("Test table found in the database. Test Passed.")


# Only execute test for the complete datapipeline if the previous tests passed
@pytest.mark.dependency(depends=["test_get_datasources", "test_store_dataframe"])
def test_datapipeline():
    """
    Tests the complete pipeline if all previous tests were successfull

    For this run the main function and check if it creates the sqlite databases with #
    the expected amount of tables
    """
    db_path_employees = '../data/employees_data.sqlite'
    db_path_rd_expenditure = '../data/R&D_Expenditure.sqlite'

    pipe.main()

    assert os.path.exists(db_path_employees), f"Database file " \
                                              f"'{db_path_employees}' does not exist."
    print(f"Database file '{db_path_employees}' "
          f"exists. Test Passed.")

    assert os.path.exists(db_path_rd_expenditure), f"Database file " \
                                                   f"'{db_path_rd_expenditure}' does not exist."
    print(f"Database file '{db_path_rd_expenditure}' "
          f"exists. Test Passed.")

    engine = create_engine(f"sqlite:///{db_path_employees}")
    inspector = inspect(engine)
    tables = inspector.get_table_names()

    assert len(tables) == 1, f"Expected 1 table in " \
                             f"employees database, found {len(tables)}."
    print(f"Number of tables "
          f"in employees database: {len(tables)}. Test Passed.")

    assert "employees" in tables, "Employees table not found " \
                                  "in employees database."
    print("Employees table found in "
          "employees database. Test Passed.")

    engine = create_engine(f"sqlite:///{db_path_rd_expenditure}")
    inspector = inspect(engine)
    tables = inspector.get_table_names()

    assert len(tables) == 1, f"Expected 1 table in " \
                             f"R&D Expenditure database, found {len(tables)}."
    print(f"Number of tables in R&D "
          f"Expenditure database: {len(tables)}. Test Passed.")

    assert "R&D_Expenditure" in tables, "R&D_Expenditure table not " \
                                        "found in R&D Expenditure database."
    print("R&D_Expenditure table found "
          "in R&D Expenditure database. Test Passed.")


if __name__ == "__main__":
    pytest.main([__file__])
