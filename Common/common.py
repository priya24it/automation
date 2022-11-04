import json
from datetime import datetime
import pandas as pd
import numpy as np
from loguru import logger
import os
import path as p
import re


def load_tables_names(module="module2"):
    data = {}
    tables_to_test = pd.DataFrame(pd.read_csv(p.tables_csv_path[module]))
    data['Tablenames'] = tables_to_test['TABLE_NAME'].to_list()
    return data


def load_snowflake_data():
    with open(f"{p.testDatapath}") as f:
        data_1 = json.load(f)
    return data_1


def format_logfile(filename):
    dir = f'logs_{datetime.now().strftime("%d%m%y")}'
    pth = f'{p.logfile_path}\\{dir}'
    if not os.path.exists(pth):
        os.makedirs(pth)
    log_file = f'{pth}\\{filename}_Automation_log_{datetime.now().strftime("%H_%M_%S")}'
    return log_file


def get_the_sql_queries():
    db_details = load_snowflake_data()

    user = db_details["snowflake_details"]['user']
    account = db_details["snowflake_details"]['account']
    role = db_details["snowflake_details"]['role']
    authenticator = db_details["snowflake_details"]['authenticator']
    snowflake_details = [user, account, role, authenticator]

    warehouse = db_details["common_sqlqueries"]["warehouse"]
    database = db_details["common_sqlqueries"]["database"]
    schema = db_details["common_sqlqueries"]["schema"]
    select = db_details["common_sqlqueries"]["select"]
    primary_key = db_details["common_sqlqueries"]["primary_key"]
    business_key = db_details["common_sqlqueries"]["business_key"]
    key_columns = db_details["common_sqlqueries"]["key_columns"]
    sqlquery = [warehouse, database, schema, select, primary_key, business_key, key_columns]
    return snowflake_details, sqlquery


def get_db_info(module="module2"):
    module = module.lower()
    detail_name = f"{module}_details"
    test_data = load_snowflake_data()
    source_database = test_data[detail_name]['source_database']
    target_database = test_data[detail_name]['target_database']
    source_schema = test_data[detail_name]['source_schema']
    target_schema = test_data[detail_name]['target_schema']
    warehouse = test_data[detail_name]['warehouse']
    db_info = [source_database, target_database, source_schema, target_schema, warehouse]
    return db_info

"""
def get_the_module2_db_info():
    test_data = load_snowflake_data()
    database = test_data["module2_stage_details"]['database']
    eds_database = test_data["module2_stage_details"]['database_eds']
    work_sch = test_data["module2_stage_details"]['work_schema']
    stage_schema = test_data["module2_stage_details"]['stage_schema']
    warehouse = test_data["module2_stage_details"]['warehouse']
    module2_db_info = [database,eds_database,work_sch,stage_schema,warehouse]
    return  module2_db_info

def get_the_module1_db_info():
    test_data = load_snowflake_data()
    database = test_data["module1_stage_details"]['database']
    eds_database = test_data["module1_stage_details"]['database_eds']
    work_sch = test_data["module1_stage_details"]['work_schema']
    stage_schema = test_data["module1_stage_details"]['stage_schema']
    warehouse = test_data["module1_stage_details"]['warehouse']
    module1_db_info = [database, eds_database, work_sch, stage_schema, warehouse]
    return module1_db_info
"""


def get_work_table_data(cursor, tn, module, custom_query=""):
    snowflake_details, sqlquery = get_the_sql_queries()
    db_info = get_db_info(module)
    """
    if module == "module2":
        db_info = get_the_module2_db_info()
    else:
        db_info = get_the_module1_db_info()
    """
    logger.info("Start the execution of getting the work table data")

    sqlquery[1] = sqlquery[1].replace('{database_name}', db_info[0])
    logger.info(f"{sqlquery[1]}")
    cursor.execute(f"{sqlquery[1]}")

    sqlquery[2] = sqlquery[2].replace('{schema_name}', db_info[2])
    logger.info(f"{sqlquery[2]}")
    cursor.execute(f"{sqlquery[2]}")

    final_sql = custom_query
    if final_sql == "":
        final_sql = sqlquery[3].replace('{table_name}', tn)
    logger.info(f"{final_sql}")
    work_table = cursor.execute(f"{final_sql}").fetch_pandas_all()

    logger.info(f"Query has executed successfully and the number of rows and columns are {work_table.shape}")
    return work_table


def get_work_table_pk(cursor,tn,module):
    snowflake_details,sqlquery = get_the_sql_queries()
    db_info = get_db_info(module)
    """
    if module == "module2":
        db_info = get_the_module2_db_info()
    else:
        db_info = get_the_module1_db_info()
    """
    logger.info("Start the execution of getting the work table primary key")
    sqlquery[1] = sqlquery[1].replace('{database_name}', db_info[0])
    logger.info(f"{sqlquery[1]}")
    cursor.execute(f"{sqlquery[1]}")

    sqlquery[2] = sqlquery[2].replace('{schema_name}', db_info[2])
    logger.info(f"{sqlquery[2]}")
    cursor.execute(f"{sqlquery[2]}")

    sqlquery[4] = sqlquery[4].replace('{table_name}', tn).replace('{schema_name}',db_info[2])
    logger.info(f"{sqlquery[4]}")
    cursor.execute(f"{sqlquery[4]}")
    work_pk = cursor.fetchone()[0]
    logger.info(f"The primary key for the given table is {work_pk}")

    return work_pk


def get_stage_table_data(cursor, tn, module):
    snowflake_details, sqlquery = get_the_sql_queries()
    db_info = get_db_info(module)
    """
    if module == "module2":
        db_info = get_the_module2_db_info()
    else:
        db_info = get_the_module1_db_info()
    """
    logger.info("Start the execution of getting the stage table data")
    sqlquery[1] = sqlquery[1].replace('{database_name}', db_info[1])
    logger.info(f"{sqlquery[1]}")
    cursor.execute(f"{sqlquery[1]}")

    sqlquery[2] = sqlquery[2].replace('{schema_name}', db_info[3])
    logger.info(f"{sqlquery[2]}")
    cursor.execute(f"{sqlquery[2]}")

    sqlquery[3] = sqlquery[3].replace('{table_name}', tn)
    logger.info(f"{sqlquery[3]}")
    stage_table = cursor.execute(f"{sqlquery[3]}").fetch_pandas_all()

    logger.info(f"Query has executed successfully and the number of rows and columns are {stage_table.shape}")
    return stage_table

def get_stage_table_pk(cursor, tn, module):
    logger.info("Start the execution of getting the stage table primary key")
    snowflake_details, sqlquery = get_the_sql_queries()
    db_info = get_db_info(module)
    """
    if module == "module2":
        db_info = get_the_module2_db_info()
    else:
        db_info = get_the_module1_db_info()
    """
    sqlquery[1] = sqlquery[1].replace('{database_name}', db_info[1])
    logger.info(f"{sqlquery[1]}")
    cursor.execute(f"{sqlquery[1]}")

    sqlquery[2] = sqlquery[2].replace('{schema_name}', db_info[3])
    logger.info(f"{sqlquery[2]}")
    cursor.execute(f"{sqlquery[2]}")

    sqlquery[4] = sqlquery[4].replace('{table_name}', tn).replace('{schema_name}', db_info[3])
    logger.info(f"{sqlquery[4]}")
    cursor.execute(f"{sqlquery[4]}")
    stage_pk = cursor.fetchone()[0]
    logger.info(f"The primary key for the given table is {stage_pk}")

    return stage_pk


def get_stage_table_bk(cursor, tn, module, custom_query=""):
    logger.info("Start the execution of getting the stage table business key")
    snowflake_details, sqlquery = get_the_sql_queries()
    db_info = get_db_info(module)

    sqlquery[1] = sqlquery[1].replace('{database_name}', db_info[1])
    logger.info(f"{sqlquery[1]}")
    cursor.execute(f"{sqlquery[1]}")

    sqlquery[2] = sqlquery[2].replace('{schema_name}', db_info[3])
    logger.info(f"{sqlquery[2]}")
    cursor.execute(f"{sqlquery[2]}")

    sqlquery[5] = sqlquery[5].replace('{table_name}', tn).replace('{schema_name}', db_info[3])
    logger.info(f"{sqlquery[5]}")
    table_bk = cursor.execute(f"{sqlquery[5]}").fetch_pandas_all()

    logger.info(f"Query has executed successfully and the number of rows and columns are {table_bk.shape}")
    return table_bk

def get_stage_table_key_columns(cursor, tn, module, custom_query=""):
    logger.info("Start the execution of getting the stage table business key")
    snowflake_details, sqlquery = get_the_sql_queries()
    db_info = get_db_info(module)

    sqlquery[1] = sqlquery[1].replace('{database_name}', db_info[1])
    logger.info(f"{sqlquery[1]}")
    cursor.execute(f"{sqlquery[1]}")

    sqlquery[2] = sqlquery[2].replace('{schema_name}', db_info[3])
    logger.info(f"{sqlquery[2]}")
    cursor.execute(f"{sqlquery[2]}")

    sqlquery[6] = sqlquery[6].replace('{table_name}', tn).replace('{schema_name}', db_info[3])
    logger.info(f"{sqlquery[6]}")
    table_key_columns = cursor.execute(f"{sqlquery[6]}").fetch_pandas_all()

    logger.info(f"Query has executed successfully and the number of rows and columns are {table_key_columns.shape}")
    return table_key_columns


def compare_rows(w_table, st):
    w_table = w_table.fillna(0)
    st = st.fillna(0)
    result_df = pd.DataFrame()
    for i, j in w_table.iteritems():
        result_df["Work:" + i] = w_table[i]
        result_df["Stage:" + i] = st[i]
        result_df["Res:" + i] = pd.Series(np.where(w_table[i] == st[i], "Match", "Unmatch"))
    changed_rows = result_df[result_df.isin(["Unmatch"]).any(axis=1)]
    return changed_rows

def count_check(source_data,process_date,st=''):
    if "PROCESS_INSERT_TMS" in source_data.columns.to_list():
        source_data_1 = source_data[source_data.PROCESS_INSERT_TMS >= process_date]
    else:
        source_data_1 = source_data
    if source_data_1.shape[0] == 0:
        logger.error("Total records are Zero, Hence marking as a failed.")
        assert 1==2,f"Total records are Zero {source_data_1.shape}, Hence marking as a failed."
    else:
        logger.info(f'Total number of records in the work table is > 0 (Total rows:{source_data_1.shape[0]}), Hence testcases has passed.')
    if st is not '':
        if "PROCESS_INSERT_TMS" in st.columns.to_list():
            st_1 = st[st.PROCESS_INSERT_TMS >= process_date]
        else:
            st_1 = st
        if source_data_1.shape[0] != st_1.shape[0]:
            logger.error(f"Total records are not matching between the work and staging tables  Total records in work table are {source_data_1.shape[0]} and Total records in stage table are {st_1.shape[0]}. Hence marking as a failed.")
            assert 1 == 2, f"Total records are not matching between the work and staging tables  Total records in work table are {source_data_1.shape[0]} and Total records in stage table are {st_1.shape[0]}. Hence marking as a failed."
        else:
            logger.info(f"Total records are matching between the work and staging tables \n Total records in work table are {source_data_1.shape[0]} and Total records in stage table are {st_1.shape[0]}"
                  f"hence testcase has passed.")

def minus_check(source_data, process_date,st='',tn=''):
    if "PROCESS_INSERT_TMS" in source_data.columns.to_list():
        source_data_1 = source_data[source_data.PROCESS_INSERT_TMS >= process_date]
    else:
        source_data_1 = source_data
    num_of_cols = source_data_1.shape[1]
    if st is '':
        logger.info(f"Total number of rows in the given table are  {source_data_1.shape[0]}")
        logger.info(f"Total number of columns in the given table are {num_of_cols}")
        w_table_1 = source_data_1.iloc[0]
        logger.info(w_table_1.to_json())
    if st is not '':
        if "PROCESS_INSERT_TMS" in st.columns.to_list():
            st_1 = st[st.PROCESS_INSERT_TMS >= process_date]
        else:
            st_1 = st
        sc = source_data_1.columns.to_list()
        tc = st_1.columns.to_list()
        logger.info(sc)
        logger.info(tc)

        cc = list(set(sc).difference(set(tc)))
        # print(list(set(sc).difference(set(tc))))
        # print(list(set(tc).difference(set(sc))))
        source_data_1 = source_data_1[cc]
        st_1 = st_1[cc]
        num_of_work_rows = source_data_1.shape[0]
        num_of_stage_rows = st_1.shape[0]
        if num_of_work_rows == num_of_stage_rows:
            changed_rows = compare_rows(source_data_1, st_1)
            if changed_rows.shape[0] > 0:
                assert 1==2, ("There are few rows are not matching between the work and stage,hence testcase has failed. Please refer generated excel for mismatched records.")
            else:
                logger.info(f"All rows are matched between the work i.e {num_of_work_rows} and stage i.e {num_of_stage_rows} tables hence testcase has passed.")
        # else:
        #     assert 1==2, f"Num of rows are not same i.e {source_data.shape},{st.shape}, please do manual verification."
        #     logger.info("Num of rows are not same , please do manual verification.")

def duplicate_check_PK_level(source_data,pk,st='',st_pk=None):
    # count = 0
    if st is '':
        count = source_data.duplicated(subset=pk).sum()
        if count > 0:
            logger.error(f"{count} duplicate records are found, please check the data manually.")
            assert 1 == 2, f"{count} duplicate records are found, please check the data manually."
        else:
            logger.info(f"Total number of duplicate records at primary key level are {count},hence testcase has passed.")
    if st is not '':
        count_st = st.duplicated(subset=st_pk).sum()
        if count_st > 0:
            logger.error(f"{count} duplicate records are found, please check the data manually and testcase has failed")
            assert 1 == 2, f"{count} duplicate records are found, please check the data manually and testcase has failed"
        else:
            logger.info(f"Total number of duplicate records at primary key level are {count_st} hence testcase has passed")

def duplicate_check_table_level(source_data,st=''):
    if st is '':
        count = source_data.duplicated().sum()
        if count > 0:
            logger.error(f"{count} duplicate records are found in work table, please check the data manually.")
            assert 1 == 2, f"{count} duplicate records are found in work table, please check the data manually."
        else:
            logger.info(f" Total number of duplicate records at table level are {count} hence testcase has passed.")
    if st is not '':
        count_st = st.duplicated().sum()
        if count_st > 0:
            logger.error(f"{count_st} duplicate records are found in stage table, please check the data manually and testcase has failed.")
            assert 1 == 2, f"{count_st} duplicate records are found in stage table, please check the data manually."
        else:
            logger.info(f"Total number of duplicate records at primary key level are {count_st} hence testcase has passed.")


def validating_key_cols(source_data):
    total_num_of_null_cols = source_data.columns[source_data.isnull().all()].to_list()
    Key_cols = [c for c in total_num_of_null_cols if str(c).endswith("KEY")]
    if len(Key_cols)  == 0:
        cols = ','.join(Key_cols)
        logger.info(f"Data is exists for the given  key columns i.e {cols},hence TC has passed")
    else:
        logger.error(f"{Key_cols} column values are null ,please verify the data manually")
        assert 1 == 2, f"{Key_cols} column values are null ,please verify the data manually "

def validating_cd_cols(source_data):
    total_num_of_null_cols = source_data.columns[source_data.isnull().all()].to_list()
    CD_cols = [c for c in total_num_of_null_cols if str(c).endswith("CD")]
    if len(CD_cols) == 0:
        cols = ','.join(CD_cols)
        logger.info(f"Data is exists for the given CD columns i.e {cols} hence testcase has passed")
    else:
        logger.error(f"{CD_cols} column values are null ,please verify the data manually")
        assert 1 == 2, f"{CD_cols} column values are null,please verify the data manually"

def validating_nonkey_cols(source_data):
    total_num_of_null_cols = source_data.columns[source_data.isnull().all()].to_list()
    Key_cols = [c for c in total_num_of_null_cols if str(c).endswith("KEY")]
    CD_cols = [c for c in total_num_of_null_cols if str(c).endswith("CD")]
    cols = list(set(total_num_of_null_cols) - set(CD_cols + Key_cols))
    if len(cols) == 0:
        logger.info(f"Data is exists for the non key columns i.e {cols} hence TC has passed")
    else:
        logger.error(f"{cols} column values are null ,please verify the data manually")
        assert 1 == 2, f"{cols} column values are null ,please verify the data manually"

def display_key_cols(source_data):
    total_cols = source_data.columns.to_list()
    Key_cols = [c for c in total_cols if str(c).endswith("KEY")]
    df_1 = pd.DataFrame()
    df_1 = source_data[Key_cols]
    df_1 = df_1.iloc[0]
    logger.info(f"Below is the key columns data for one record ==> \n {df_1.to_json()}")

def display_CD_cols(source_data):
    total_cols = source_data.columns.to_list()
    CD_cols = [c1 for c1 in total_cols if str(c1).endswith("CD")]
    df_1 = pd.DataFrame()
    df_1 = source_data[CD_cols]
    df_1 = df_1.iloc[0]
    logger.info(f"Below is the CD columns data for one record ==> \n {df_1.to_json()}")


def display_nonKey_cols(source_data):
    total_cols = source_data.columns.to_list()
    Key_cols = [c for c in total_cols if str(c).endswith("KEY")]
    CD_cols = [c1 for c1 in total_cols if str(c1).endswith("CD")]
    non_key_cols = list(set(total_cols) - set(CD_cols + Key_cols))
    df_1 = pd.DataFrame()
    df_1 = source_data[non_key_cols]
    df_1 = df_1.iloc[0]
    logger.info(f"Below is the non key  columns data for one record ==> \n {df_1.to_json()}")


def validate_pl_LOB_data(source_data):
    pl_lobs = [
        "BOATOWNER",
        "DWELLINGFIRE",
        "FAMILYAUTO",
        "MOTORCYCLE",
        "PAPRODUCT",
        "PERSONALINLANDMARINE",
        "PINPOINTHOMEOWNERS"
    ]
    res = source_data.isin(pl_lobs).any().any()
    if res:
        assert 1==2,"Personal lines data is exists, hence testcase marked as fail"
        logger.error("Personal lines data is exists, hence testcase marked as fail")
    else:
        logger.info(f"Personal lines data i.e {pl_lobs} \n does not exists, hence testcase has passed.")


def SS_validation(source_data):
    df = source_data[source_data["SOURCE_SYSTEM"] != "GWPC"]
    if df.shape[0] > 0:
        assert 1==2,"Few of the  records are not present with the GWPC code ,hence it got failed"
        logger.error("Few of the  records are not present with the GWPC code, hence testcase has failed.")
    else:
        logger.info("All the records are present with the GWPC code, so no issue here")
    return df.shape[0]


def data_truncation(table_name1, ind="KEY"):
    pass


def incremental_load_count(source_data,process_date): #process_date = "2022-05-24 00:00:00.000"

    if "PROCESS_UPDATE_KEY" in source_data.columns.to_list():
        df_2 = source_data["PROCESS_UPDATE_KEY"].value_counts()
        logger.info(df_2.to_json())
    else:
        df_2 = source_data["PROCESS_INSERT_TMS"].value_counts()
        logger.info(df_2.to_json())

    df_1 = source_data[source_data.PROCESS_INSERT_TMS >= process_date]
    if df_1.shape[0] == 0:
        assert 1==2,"Total number of records in the incremental load is Zero, Hence TC has failed."
        logger.info("Total number of records in the incremental load is Zero, Hence TC has failed.")
    else:
        logger.info(f"Total number of records in the incremental load are {df_1.shape[0]}")


def display_incremental_data(source_data,process_date):
    df_1 = source_data[source_data.PROCESS_INSERT_TMS >= process_date]
    df_1 = df_1.iloc[0]
    logger.info(f" Below is the data for one  record which has loaded as part of the incremental process ==> \n {df_1.to_json()}")


def duplicate_check_bk_level(source_data, bk_data):
    count = source_data.duplicated(subset=bk_data["COLUMN_NAME"].tolist()).sum()
    if count > 0:
        logger.error(f"{count} duplicate records are found in the BK level, please check the data manually.")
        assert 1 == 2, f"{count} duplicate records are found in the BK level, please check the data manually."
    else:
        logger.info(f"Total number of duplicate records at business key level is {count}, hence testcase has passed.")


def minus_check_key_columns_level(source_data, target_data, key_cols):
    df_source = source_data[key_cols]
    df_target = target_data[key_cols]
    df_compare = df_target.merge(df_source, indicator=True, how='left').loc[lambda x: x['_merge'] != 'both']
    if len(df_compare) > 0:
        msg = f"Different records were found when comparing {key_cols} key columns, please check data manually."
        logger.error(msg)
        assert 1 == 2, msg
    else:
        msg = f"Records in {key_cols} key columns of source and target data are equal, hence testcase has passed."
        logger.info(msg)

