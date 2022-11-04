import pytest
import Common.common as c
import Common.matillion_api as m
from loguru import logger
from getpass import getpass

module2_details = c.load_snowflake_data()
process_date = module2_details["module3_module1_details"]["process_date"]
matillion_user = input("Enter your Matillion username: ")
matillion_passwd = getpass("Enter your Matillion password: ")


@pytest.mark.usefixtures('cursor')
class TestEdsWorkmodule1:
    pytest.source_table = ''
    pytest.source_pk = ''
    pytest.target_table = ''
    pytest.target_pk = ''
    pytest.handler_id = 0
    pytest.flag = ''
    pytest.target_bk = ''
    pytest.target_key_columns = ''
    pytest.target_key_columns_list = []

    @pytest.fixture
    def _setup(self, cursor, tn):
        if pytest.handler_id > 0:
            logger.remove(pytest.handler_id)
        log_filename = c.format_logfile(filename=tn)
        pytest.handler_id = logger.add(f'{log_filename}.log')
        source_query = m.get_query_from_matillion_job("module1", tn, matillion_user, matillion_passwd, process_date)
        pytest.source_table = c.get_work_table_data(cursor, tn, module="module3_module1", custom_query=source_query)
        pytest.target_table = c.get_stage_table_data(cursor, tn, module="module3_module1")
        pytest.target_pk = c.get_stage_table_pk(cursor, tn, module="module3_module1")
        pytest.target_bk = c.get_stage_table_bk(cursor, tn, module="module3_module1")
        pytest.target_key_columns = c.get_stage_table_key_columns(cursor, tn, module="module3_module1")
        pytest.target_key_columns_list = pytest.target_key_columns["COLUMN_NAME"].to_list()

    @pytest.mark.usefixtures('_setup')
    def test_tc001_count_testing(self, tn):
        logger.info("==================================================")
        logger.info(f"Execution has started for count check -- {tn}")
        source_tb = pytest.source_table
        target_tb = pytest.target_table
        c.count_check(source_data=source_tb, process_date=process_date, st=target_tb)

    def test_tc002_minus_testing(self, tn):
        if pytest.source_table.shape[0] == 0:
            pytest.skip("The number of records in source table is Zero hence skipping the TC")
        else:
            logger.info(pytest.flag)
            logger.info("==================================================")
            logger.info(f"Execution has started for data checks. -- {tn}")
            source_tb = pytest.source_table
            target_tb = pytest.target_table
            c.minus_check(source_data=source_tb, process_date=process_date, st=target_tb, tn=tn)

    def test_tc003_dup_check_bk(self, tn):
        if pytest.source_table.shape[0] == 0:
            pytest.skip("The number of records in source table is Zero hence skipping the TC")
        else:
            logger.info(pytest.flag)
            logger.info("==================================================")
            logger.info(f"Execution has started to verify the duplicate rows at business key level. -- {tn}")
            tb = pytest.target_table
            bk = pytest.target_bk
            c.duplicate_check_bk_level(source_data=tb, bk_data=bk)

    def test_tc004_minus_key_columns(self, tn, key_cols):
        if pytest.source_table.shape[0] == 0:
            pytest.skip("The number of records in source table is Zero hence skipping the TC")
        else:
            logger.info(pytest.flag)
            logger.info("==================================================")
            logger.info(f"Execution has started for data checking {key_cols} column. -- {tn}")
            source_tb = pytest.source_table
            target_tb = pytest.target_table
            final_key_cols = ["module1_ID", key_cols]
            c.minus_check_key_columns_level(source_data=source_tb, target_data=target_tb, key_cols=final_key_cols)

