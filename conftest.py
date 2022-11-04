import pytest
import snowflake.connector
import Common.common as c
from loguru import logger

@pytest.fixture(scope='module')
def cnxn():
    snowflake_details, sqlquery = c.get_the_sql_queries()
    cnxn = snowflake.connector.connect(
        user=snowflake_details[0],
        account=snowflake_details[1],
        role=snowflake_details[2],
        authenticator=snowflake_details[3],
    )
    yield cnxn
    cnxn.close()

@pytest.fixture(scope="module")
def cursor(cnxn):
    snowflake_details, sqlquery = c.get_the_sql_queries()
    db_info = c.get_db_info()
    cursor = cnxn.cursor()
    sqlquery[0] = sqlquery[0].replace('{warehouse_name}', db_info[4])
    logger.info(f"{sqlquery[0]}")
    cursor.execute(f"{sqlquery[0]}")
    yield cursor
    cnxn.rollback()


def pytest_collection_modifyitems(config, items):
    """Modifies test items in place to ensure test modules run in a given order"""
    def param_part(item):
        logger.info(item.nodeid)
        if item.nodeid.startswith("test_stage_module1.py::Testmodule1::"):
            index = item.nodeid.find('[')
            if index > 0:
                return item.nodeid[item.nodeid.index('['):]
        if item.nodeid.startswith("test_stage_module2.py::Testmodule2::"):
            index = item.nodeid.find('[')
            if index > 0:
                return item.nodeid[item.nodeid.index('['):]
        if item.nodeid.startswith("test_module3_module1.py::TestEdsWorkmodule1::"):
            index_param_value = item.nodeid.find('[')
            if index_param_value > 0:
                tb_param_name = item.nodeid[item.nodeid.index('['):]
                test_name = item.nodeid[item.nodeid.index('::test_')+2:item.nodeid.index('[')]
                if tb_param_name.find('-') > 0:
                    tb_name = tb_param_name[:tb_param_name.index('-')] + "]"
                    param_name = tb_param_name[tb_param_name.index('-'):tb_param_name.index(']')]
                else:
                    tb_name = tb_param_name
                    param_name = ""
                item_final_name = tb_name + '::' + test_name + '::' + param_name
                return item_final_name
        return item.nodeid

    items[:] = sorted(items, key=param_part)


def _cnxn():
    snowflake_details, sqlquery = c.get_the_sql_queries()
    cnxn = snowflake.connector.connect(
        user=snowflake_details[0],
        account=snowflake_details[1],
        role=snowflake_details[2],
        authenticator=snowflake_details[3],
    )
    snowflake_details, sqlquery = c.get_the_sql_queries()
    db_info = c.get_db_info()
    cursor = cnxn.cursor()
    sqlquery[0] = sqlquery[0].replace('{warehouse_name}', db_info[4])
    logger.info(f"{sqlquery[0]}")
    cursor.execute(f"{sqlquery[0]}")
    return cnxn
    # cnxn.close()


global_conn = _cnxn()


def pytest_generate_tests(metafunc):
    class_name = metafunc.cls.__name__.split('_')[0]
    logger.info(f"Collection Phase:: Initializing variables for {class_name}...")
    db_details = c.load_tables_names(module="module3_module1")
    table_names = db_details["Tablenames"]
    if "tn" in metafunc.fixturenames:
        if "key_cols" in metafunc.fixturenames:
            params = []
            for tn in table_names:
                cursor = global_conn.cursor()
                df_key_cols = c.get_stage_table_key_columns(cursor, tn, module="module3_module1")
                key_cols = df_key_cols["COLUMN_NAME"].to_list()
                for i in key_cols:
                    params.append((tn, i))
            global_conn.close()
            metafunc.parametrize(["tn", "key_cols"], params)
        else:
            metafunc.parametrize("tn", table_names)
