The purpose of the automation framework is provide the maximum test coverage 
and reduce the manual efforts.



Common
Libs
TestData
Logs
Reports

Conftest
Test_module1.py
Test_module2.py
Test_module2.py

requirments.txt

TestData: User-defined libraries
Libs: Custom-defined methods which are accessible across all the modules. 
Basically, these are the reusable components. 
Logs: For each TC, recorded the log file for each and every test step.
Reports: The generated Pytest HTML Report and Pytest XML Report will be placed here.
Common: Define the functions which are common to all the test modules.
i.e load_the_json_file. 
Conftest -- Define the fixture functions in this file to make them accessible across multiple test files.
Below are the test files which starts the execution of the program. 
Test_module1.py
Test_module2.py
Test_module2.py

Below command is useful to trigger the execution. 
pytest Test_module1.py --html="report.html" --xml="report.xml"
pip install -r requirments.txt




