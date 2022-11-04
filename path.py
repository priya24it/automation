from pathlib import Path
import os

parentpath = Path(__file__).parents[0]
logfile_path = os.path.join(parentpath, 'Logs')
testDatapath = os.path.join(parentpath, 'TestData', 'test_data.json')
tables_csv_path = {
    "module2": os.path.join(parentpath, 'TestData', 'module2_tables_to_test.csv'),
    "module1": os.path.join(parentpath, 'TestData', 'module1_tables_to_test.csv'),
    "module3_module1": os.path.join(parentpath, 'TestData', 'module3_tables_to_test.csv')
}

"""
module2_tables_csv_path = os.path.join(parentpath, 'TestData', 'module2_tables_to_test.csv')
module1_tables_csv_path = os.path.join(parentpath, 'TestData', 'module1_tables_to_test.csv')
module3_tables_csv_path = os.path.join(parentpath, 'TestData', 'module3_tables_to_test.csv')
"""
