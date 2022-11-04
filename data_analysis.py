# The purpose of the program is analysis the text file data. 
# It displays the data in below order,
# 1. It calculates  the number of non-null-columns and it's count. 
# 2. It calculates  the number of duplicate rows at file level and it's keys. 
# 3. It calculates  the occurrence  of each value in a file where column name ends with "FL","CD","SYSTEM"
# 4. It appends the top 3 rows where column name ends with "Key" in a file to the excel.
# 5. It appends the top 3 rows where column name does not end with "Key" in a file to the excel.
# The generated  output has same name as filename. 

import pandas as pd
import openpyxl
import common as c
import os

#Create the instance of excel workbook.
wb = openpyxl.Workbook()
ws = wb.active

# Please provide the Input text file.
list_of_files = os.listdir("TestData/")

for f in range(0,len(list_of_files)):
  extenstion_type= list_of_files[f].split(".")
  print(extenstion_type)
  
  # Below code finds the respective folders path in the project.
  parent_path = os.getcwd() 
  test_data_path = os.path.join(parent_path,"TestData")
  output_path = os.path.join(parent_path,"output")
  
  if len(extenstion_type)>0: 
    # The read_csv function is used to read the text files,delimiter is an important parameter and it should separated  with | symbol.
    if extenstion_type[1] == "csv":
      sourcedata = c.read_csv(test_data_path,list_of_files[f])
      source_data = pd.DataFrame(sourcedata)
      filename = extenstion_type[0]
    elif extenstion_type[1] == "txt":
      sourcedata = c.read_text(test_data_path,list_of_files[f])
      f = list_of_files[f].split("_")
      filename = f[3]
      source_data = pd.DataFrame(sourcedata)
    elif extenstion_type[1] == "xlsx":
      filename = extenstion_type[0]
      sourcedata = c.read_excel(test_data_path,list_of_files[f])
      source_data = pd.DataFrame(sourcedata)
    else:
      print("Please import the valid file which has either .txt,.csv,.xlsx extension") 
    
    # Create the sheet name with the same filename.
    ws = wb.create_sheet(filename)
    
    #The columns property returns the label of each column in the textfile and converting into the list variable.
    list_of_cols = source_data.columns.to_list()
    # print(','.join(list_of_cols))
    
    #get_dataframe_info() function calculates the  number of columns, column labels, column data types and the non-null-counts.
    # Once we get the data we store this in info variable and will append to the excel.
    info = c.get_dataframe_info(source_data)
    df_info = c.change_datatype(info)
    col_names = c.append_data_to_excel(data = df_info,work_sheet=ws)
    c.add_background_color(ws,col_names)
    
    
    #Below code finds the column names which has all null values and apply Red color font to it.
    null_c = source_data.columns[source_data.isnull().all()].to_list()
    null_columns = list(set(null_c).difference(set(list_of_cols)))
    df_null_cols = df_info[df_info["column_names"].isin(null_c)]
    list_of_indexs = df_null_cols.index
    required_indexs = []
    m = 0
    
    for index in list_of_indexs:
      required_indexs.append("A"+str(index+3))
      required_indexs.append("B"+str(index+3))
      m = m+1
    c.change_font_color_to_red(ws,required_indexs)
    
    # Below code calculates the number of duplicate rows and it's key at file level.
    d1 = {}
    duplicate_count = source_data.duplicated().sum()
    dup_keys = ''
    
    if duplicate_count > 0:
      d1["Number of Duplicate Rows"] = [duplicate_count]
      duplicate_source_rows = source_data[source_data.duplicated()]
      dup_keys = duplicate_source_rows[list_of_cols[0]].to_list()
      d1["One of the Duplicate key is"] = [dup_keys[0]]
    else:
      d1["Number of Duplicate Rows"] = [0]
      d1["One of the Duplicate key is"] = ["No Duplicates"]
    
    # Below code append the duplicate data into the excel.
    dup_data = pd.DataFrame(d1)
    col_names = c.append_data_to_excel(data = dup_data,work_sheet=ws)
    c.add_background_color(ws,col_names)
    
    # Below code find the column names in a file which ends with "FL"
    FL_cols = [fl for fl in list_of_cols if str(fl).endswith("FL")]
    
    # Below code find the column names in a file which ends with "FL"
    IND_cols = [fl for fl in list_of_cols if str(fl).endswith("IND")]
    
    # Below code find the column names in a file which ends with "CD"
    CD_cols = [c for c in list_of_cols if str(c).endswith("CD")]
    
    # Below code find the column names in a file which ends with "KEY"
    Key_cols = [c for c in list_of_cols if str(c).endswith("KEY")]
    
    # Below code find the column names in a file which ends with "SYSTEM"
    ss = [c for c in list_of_cols if str(c).endswith("SYSTEM")]
    
    # Below code find the non key columns in a file
    non_key_cols = list(set(list_of_cols)- set(FL_cols+CD_cols+Key_cols+ss+IND_cols))
    
    # Below code calculates  occurrence of datavalue which column name ends with "FL","CD","SYSTEM".
    category_cols = FL_cols + CD_cols+ss+IND_cols
    for f in range(0,len(category_cols)):
      df_values = c.cal_of_unique_values(source_data,category_cols[f])
      col_names = c.append_data_to_excel(data = df_values,work_sheet = ws)
      c.add_background_color(ws,col_names)
      
    # Below is the code to fetch the Top 3 rows in the file where column name ends with Key.
    key_data = c.get_the_first_three_rows(source_data,Key_cols)
    col_names = c.append_data_to_excel(data = key_data,work_sheet = ws)
    c.add_background_color(ws,col_names)
    
    # Below is the code to fetch the Top 3 rows in the file for all non key cols.
    non_key_data = c.get_the_first_three_rows(source_data,non_key_cols)
    col_names = c.append_data_to_excel(data = non_key_data,work_sheet = ws)
    c.add_background_color(ws,col_names)
    
    # formatting the excel file.
    c.format_excel(ws)
    del df_info
    col_names.clear()
    del source_data
    
    # Change the current directory to output folder and save the excel.
    
    
  else:
     print("Please import the valid file which has either .txt,.csv,.xlsx extension")
try:
  os.chdir(output_path)
except:
  pass
wb.save("Data_analysis.xlsx")



