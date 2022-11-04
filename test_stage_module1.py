import pytest
import Common.common as c
from loguru import logger
# import time

db_details = c.load_tables_names(module="module1") #module1
table_names = db_details["Tablenames"]
module1_details = c.load_snowflake_data()
process_date = module1_details["module1_stage_details"]["process_date"]

@pytest.mark.parametrize('tn', table_names)
@pytest.mark.usefixtures('cursor')
class Testmodule1:
    pytest.work_table = ''
    pytest.work_pk = ''
    pytest.stage_table = ''
    pytest.stage_pk = ''
    pytest.handler_id = 0
    pytest.flag = ''

    @pytest.fixture
    def _setup(self, cursor, tn):
        if pytest.handler_id > 0:
            logger.remove(pytest.handler_id)
        log_filename = c.format_logfile(filename=tn)
        pytest.handler_id = logger.add(f'{log_filename}.log')
        pytest.work_table = c.get_work_table_data(cursor, tn,module="module1")
        pytest.work_pk = c.get_work_table_pk(cursor, tn,module="module1")
        pytest.stage_table = c.get_stage_table_data(cursor, tn,module="module1")
        pytest.stage_pk = c.get_stage_table_pk(cursor, tn,module="module1")

    @pytest.mark.usefixtures('_setup')
    @pytest.mark.smoke
    def test_count_testing(self, tn):
        logger.info("==================================================")
        logger.info(f"Execution has started for count check -- {tn}")
        df = pytest.work_table
        stage_table = pytest.stage_table
        c.count_check(source_data=df, process_date=process_date, st=stage_table)

    def test_minus_testing(self, tn):
        if pytest.work_table.shape[0] == 0:
            pytest.skip("The number of records in work table is Zero hence skipping the TC")
        else:
            logger.info(pytest.flag)
            logger.info("==================================================")
            logger.info(f"Execution has started for data checks. -- {tn}")
            df = pytest.work_table
            stage_table = pytest.stage_table
            c.minus_check(source_data=df, process_date=process_date, st=stage_table, tn=tn)

    def test_dup_check_pk(self, tn):
        if pytest.work_table.shape[0] == 0:
            pytest.skip("The number of records in work table is Zero hence skipping the TC")
        else:
            logger.info("==================================================")
            logger.info(f"Execution has started to verify the duplicate rows at primary key level  -- {tn}")
            df = pytest.work_table
            pk = pytest.work_pk
            stage_table = pytest.stage_table
            stage_pk = pytest.stage_pk
            c.duplicate_check_PK_level(df, pk, st=stage_table, st_pk=stage_pk)

    def test_dup_check_table_wt(self, tn):
        if pytest.work_table.shape[0] == 0:
            pytest.skip("The number of records in work table is Zero hence skipping the TC")
        else:
            logger.info("==================================================")
            logger.info(f"Execution has started to verify the duplicate rows at table level  -- {tn}")
            df = pytest.work_table
            stage_table = pytest.stage_table
            c.duplicate_check_table_level(df, st=stage_table)

    @pytest.mark.smoke
    def test_validating_key_cols(self, tn):
        if pytest.work_table.shape[0] == 0:
            pytest.skip("The number of records in work table is Zero hence skipping the TC")
        else:
            logger.info("==================================================")
            logger.info(f"Execution has started to verify the  KEY columns which are null in the table  -- {tn}")
            df = pytest.work_table
            df = pytest.stage_table
            c.validating_key_cols(df)

    def test_validating_cd_cols_st(self, tn):
        if pytest.work_table.shape[0] == 0:
            pytest.skip("The number of records in work table is Zero hence skipping the TC")
        else:
            logger.info("==================================================")
            logger.info(f"Execution has started to verify the CD columns which are null in the table  -- STAGE.{tn}")
            df = pytest.stage_table
            c.validating_cd_cols(df)
            df = pytest.work_table
            c.validating_cd_cols(df)

    def test_validating_nonkey_col(self, tn):
        if pytest.work_table.shape[0] == 0:
            pytest.skip("The number of records in work table is Zero hence skipping the TC")
        else:
            logger.info("==================================================")
            logger.info(f"Execution has started to verify the NON KEY columns which are null in the table  -- {tn}")
            df = pytest.work_table
            c.validating_nonkey_cols(df)
            df = pytest.stage_table
            c.validating_nonkey_cols(df)

    def test_validate_pl_lob_data(self, tn):
        if pytest.work_table.shape[0] == 0:
            pytest.skip("The number of records in work table is Zero hence skipping the TC")
        else:
            logger.info("==================================================")
            logger.info(f"Execution has started to verify the personal lines LOB data in the table  -- {tn}")
            df = pytest.work_table
            c.validate_pl_LOB_data(df)
            df = pytest.stage_table
            c.validate_pl_LOB_data(df)

    def test_ss_validation(self, tn):
        if pytest.work_table.shape[0] == 0:
            pytest.skip("The number of records in work table is Zero hence skipping the TC")
        else:
            logger.info("==================================================")
            logger.info(f"Execution has started to verify the source system column  data in the table  -- {tn}")
            df = pytest.work_table
            c.SS_validation(df)


