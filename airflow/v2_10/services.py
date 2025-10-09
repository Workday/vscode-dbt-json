from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.operators.bash import BashOperator
from datetime import date, datetime, timedelta, timezone
from dbt.cli.main import dbtRunner
import json
from kubernetes import client, config
import logging
import os
import time
from trino.auth import BasicAuthentication
from trino.dbapi import connect
import yaml


from _ext_.utils import (
    parse_dbt_results,
    sql_dbt_model_dates_merge,
    sql_dbt_test_dates_merge,
)
from _ext_.variables import (
    dbt_project,
    dbt_threads,
    exclude_models,
    is_local,
    k8s_config_path,
    k8s_deployment_name,
    k8s_namespace,
    override_models,
    override_sources,
    trino_catalog,
    trino_host,
    trino_password,
    trino_schema,
    trino_user,
)

log = logging.getLogger(__name__)

# Set up environment variables
os.environ["TRINO_CATALOG"] = trino_catalog
os.environ["TRINO_SCHEMA"] = trino_schema


def airflow_timeout(start_timestamp: str, timeout_minutes: int):
    start_datetime = datetime.strptime(start_timestamp, "%Y-%m-%d %H:%M:%S.%f").replace(
        tzinfo=timezone.utc
    )
    current_datetime = datetime.now(timezone.utc)
    if (start_datetime + timedelta(minutes=timeout_minutes)) < current_datetime:
        # Start skipping tasks if we are past the timeout
        raise AirflowSkipException("Timeout reached, skipping task")


def dbt_init(context):
    """
    Initialize dbt environment for the runner

    :param context: Context object from the task
    """
    # Source etl dag is in the _ext_ subdirectory
    dir_airflow_dags_ext = os.path.dirname(os.path.abspath(__file__))
    # Dags folder is one level up from the _ext_ directory
    dir_airflow_dags = os.path.join(dir_airflow_dags_ext, os.pardir)

    if is_local:
        # When running locally, we can write to the airflow directory
        dbt_project_dir = os.path.join(dir_airflow_dags, "dbt")
        dbt_profiles_dir = os.path.join(dir_airflow_dags, ".dbt")
    else:
        # When running on aws, we can only write to the tmp directory
        dir_tmp = "/tmp"
        dbt_project_dir = os.path.join(dir_tmp, "dbt")
        dbt_profiles_dir = os.path.join(dir_tmp, ".dbt")
        dir_airflow_dbt = os.path.join(dir_airflow_dags, "dbt")
        # Remove old tmp files and copy new ones into tmp directory
        task_remove_and_copy_dbt_files = BashOperator(
            task_id="remove_and_copy_dbt_files",
            bash_command=f"rm -R {dir_tmp}/* && cp -R {dir_airflow_dbt} {dir_tmp}",
        )
        task_remove_and_copy_dbt_files.execute(context=context)

    # Write a dbt profile file so the dbt runner can read it
    try:
        os.makedirs(dbt_profiles_dir)
    except:
        log.info(f"Directory {dbt_profiles_dir} already exists")
        # Already exists, fail silently
        pass
    dbt_profile_path = os.path.join(dbt_profiles_dir, "profiles.yml")
    dbt_profile_config = {
        dbt_project: {
            "outputs": {
                dbt_project: {
                    "database": trino_catalog,
                    "host": trino_host,
                    "http_schema": "https",
                    "method": "ldap",
                    "password": trino_password,
                    "port": 443,
                    "schema": trino_schema,
                    "threads": dbt_threads,
                    "type": "trino",
                    "user": trino_user,
                }
            },
            "target": dbt_project,
        }
    }
    with open(dbt_profile_path, "w") as dbt_profile_file:
        yaml.dump(
            dbt_profile_config,
            dbt_profile_file,
            default_flow_style=False,
            sort_keys=False,
        )

    # Potentially set env variables if they can be reliably used by the runner in future tasks
    # os.environ["DBT_PROFILES_DIR"] = dbt_profiles_dir
    # os.environ["DBT_PROJECT_DIR"] = dbt_project_dir

    return {"dbt_profiles_dir": dbt_profiles_dir, "dbt_project_dir": dbt_project_dir}


def dbt_invoke(args: list[str], context):
    """
    Initialize dbt environment for the runner

    :param args: List of arguments to pass to the dbt runner
    :param context: Context object from the task
    """

    dbt_dirs = dbt_init(context=context)
    dbt_profiles_dir = dbt_dirs["dbt_profiles_dir"]
    dbt_project_dir = dbt_dirs["dbt_project_dir"]

    args.extend(
        [
            "--profiles-dir",
            dbt_profiles_dir,
            "--project-dir",
            dbt_project_dir,
        ]
    )

    return dbtRunner().invoke(args)


def dbt_build(
    context: dict,  # Context object from the airflow task
    etl_timestamp: str,
    event_dates: list[str],
    select_list: list[str],
):
    log.info("STARTING DBT BUILD")
    log.info(f"SELECT LIST: {select_list}")
    log.info(f"EVENT DATES: {event_dates}")
    intersection_overrides: list[str] = []
    for model_id in override_models:
        intersection_overrides.append(model_id)
    for source_id in override_sources:
        intersection_overrides.append(f"source:{source_id}")

    if len(intersection_overrides) > 0:
        log.info(f"Overriding models and sources: {intersection_overrides}")
        intersection_string = ",".join(intersection_overrides)
        select_list = [f"{select},{intersection_string}" for select in select_list]

    dbt_select = " ".join(select_list)
    dbt_vars = json.dumps({"event_dates": ",".join(event_dates)})
    dbt_args: list[str] = [
        "build",
        "--vars",
        dbt_vars,
        "--select",
        dbt_select,
        "--exclude",
        "tag:mart",
    ]
    for model_name in exclude_models:
        if model_name:
            log.info(f"Excluding model: {model_name}")
            dbt_args.append("--exclude")
            dbt_args.append(model_name)

    build_results = dbt_invoke(
        dbt_args,
        context,
    )

    parsed_results = parse_dbt_results(
        event_dates=event_dates, run_response=build_results
    )

    model_id_dates_list = parsed_results["model_id_dates_list"]
    raise_fail_exception = parsed_results["raise_fail_exception"]
    test_id_dates_list = parsed_results["test_id_dates_list"]

    model_dates_merge_sql = sql_dbt_model_dates_merge(
        database_name=trino_catalog,
        etl_timestamp=etl_timestamp,
        event_dates=event_dates,
        model_id_dates_list=model_id_dates_list,
    )
    if model_dates_merge_sql:
        trino_run(model_dates_merge_sql)

    test_dates_merge_sql = sql_dbt_test_dates_merge(
        database_name=trino_catalog,
        etl_timestamp=etl_timestamp,
        event_dates=event_dates,
        test_id_dates_list=test_id_dates_list,
    )
    if test_dates_merge_sql:
        trino_run(test_dates_merge_sql)

    if raise_fail_exception:
        raise AirflowFailException("One or more models failed to run successfully")


def k8s_scale(workers: int):
    if is_local or not k8s_config_path or not k8s_deployment_name or not k8s_namespace:
        log.error(
            f"Skipping worker scaling because k8s_config_path, k8s_deployment_name and k8s_namespace must all be set in the airflow variables and not running locally"
        )
        return
    config.load_kube_config(config_file=k8s_config_path)
    log.info(
        f"Scaling workers to {workers} in namespace {k8s_namespace} and deployment {k8s_deployment_name}"
    )
    api_instance = client.AppsV1Api()
    body = {"spec": {"replicas": workers}}
    api_instance.patch_namespaced_deployment_scale(
        name=k8s_deployment_name, namespace=k8s_namespace, body=body
    )


def trino_run(sql: str, sql_retry: str = None):
    auth = BasicAuthentication(trino_user, trino_password)
    conn = connect(
        auth=auth,
        catalog=trino_catalog,
        host=trino_host,
        http_scheme="https",
        port=443,
        schema=trino_schema,
        user=trino_user,
    )
    cur = conn.cursor()
    try:
        log.info(sql)
        cur.execute(sql)
    except:
        if sql_retry is not None:
            time.sleep(2)
            log.info(sql_retry)
            cur.execute(sql_retry)
            time.sleep(3)
        else:
            time.sleep(5)
        cur.execute(sql)
    cur.execute(sql)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows
