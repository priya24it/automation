import requests
import re
from loguru import logger

_headers = {
    'Content-Type': 'application/json',
}
_env_variables = {
    "SNOWFLAKE_module1_DB_NAME": "SNOWFLAKE_module1_DB_NAME",
    "SNOWFLAKE_STAGE_DB_NAME": "SNOWFLAKE_STAGE_DB_NAME",
    "SNOWFLAKE_STAGE_SCHEMA": "SNOWFLAKE_STAGE_SCHEMA",
    "SNOWFLAKE_EDS_DB_NAME": "SNOWFLAKE_EDS_DB_NAME",
    "SNOWFLAKE_module3_DB_NAME": "SNOWFLAKE_module3_DB_NAME",
    "SNOWFLAKE_EDS_KEY_SCHEMA": "SNOWFLAKE_EDS_KEY_SCHEMA"
}
_url = "http://matillion-nonprod.grangeinsurance.com/rest/v1"
_group_name = "group_name"
_proj_name = "proj_name"


def get_query_from_matillion_job(topic_name, table_name, user, passwd, process_date,
                                 headers=_headers, env_variables=_env_variables, url=_url,
                                 group_name=_group_name, proj_name=_proj_name):
    job_name = f"{topic_name}_{table_name}_S_To_S1_Load"
    api_url = f"{url}/group/name/{group_name}/project/name/{proj_name}/version/name/default/job/name/{job_name}/export"
    resp = requests.get(api_url, headers=headers, verify=False, auth=(user, passwd))
    resp_json = resp.json()
    logger.info(f"The HTTP response from the Matillion API was {resp.status_code}.")

    """
    Getting the JSON part that represents the components of the job.
    """
    components = []
    for i in resp_json["objects"][0]["jobObject"]["components"]:
        components.append(resp_json["objects"][0]["jobObject"]["components"][i])

    """
    Looking for the component that has the SQL query that is used to load the work table.
    The name of the component is the key here.
    """
    sql_query = ""
    for i in components:
        comp_name = i["parameters"]["1"]["elements"]["1"]["values"]["1"]["value"]
        if "load work" in comp_name.lower() and table_name in comp_name.upper():
            sql_query = i["parameters"]["2"]["elements"]["1"]["values"]["1"]["value"]
            break

    """
    Keep only the part related to the Select instruction in the query (remove the part related to the insert into).
    """
    select_position = 0
    p = re.compile("SELECT")
    for m in p.finditer(sql_query.upper()):
        select_position = m.start()
        break
    select_sql_query = sql_query[select_position:]

    """
    Replace the variables in the SQL query considering the own job variables (got from the JSON response)
    and the environment variables (configured in env_variables).
    """
    variables = resp_json["objects"][0]["jobObject"]["variables"]
    select_sql_query = _replace_variables(select_sql_query, variables, process_date, True)
    select_sql_query = _replace_variables(select_sql_query, env_variables, process_date, False)

    return select_sql_query


def _replace_variables(text, variables_obj, process_date, using_job_variables=False):
    for v in variables_obj:
        var_name = v
        if using_job_variables:
            var_value = variables_obj[v]["value"] if "value" in variables_obj[v] else ""
        else:
            var_value = variables_obj[v]
        if var_name == "LOAD_DATE_TIME" and var_value == "":
            var_value = process_date
        text = text.replace("${" + var_name + "}", var_value)
    return text
