from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.operators.bash import BashOperator
from airflow.utils.email import send_email
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
    get_model_name_from_id,
    parse_dbt_results,
    should_retry,
    sql_dbt_model_dates_merge,
    sql_dbt_test_dates_merge,
)
from _ext_.variables import (
    custom_failure_notifications,
    dbt_project,
    dbt_threads,
    etl_schema,
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
    """
    Raise AirflowSkipException if the DAG has been running longer than the configured timeout.
    """
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
    """
    Execute a dbt build for the given models/sources and merge run results into ETL metadata tables.

    After the initial build, if any failures are not known to be permanent (e.g.
    compilation errors, missing columns), an immediate ``dbt retry`` is executed
    using the same project/profiles directories so that ``target/run_results.json``
    is preserved.  The retry results are merged into the metadata tables, overwriting
    the earlier error entries via the MERGE statement.  Only failures that match
    ``PERMANENT_ERROR_PATTERNS`` (syntax errors, permission denied, etc.) skip the
    retry since they would never self-resolve.
    """
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

    # Initialize dbt dirs once — we reuse them for any immediate retry so
    # target/run_results.json is preserved (dbt_invoke would re-init and wipe it).
    dbt_dirs = dbt_init(context=context)
    dbt_profiles_dir = dbt_dirs["dbt_profiles_dir"]
    dbt_project_dir = dbt_dirs["dbt_project_dir"]
    dir_args = ["--profiles-dir", dbt_profiles_dir, "--project-dir", dbt_project_dir]

    build_results = dbtRunner().invoke(dbt_args + dir_args)

    # ── Merge initial results into metadata tables ──────────────────────
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
        etl_schema=etl_schema,
    )
    if model_dates_merge_sql:
        trino_run(model_dates_merge_sql)

    test_dates_merge_sql = sql_dbt_test_dates_merge(
        database_name=trino_catalog,
        etl_timestamp=etl_timestamp,
        event_dates=event_dates,
        test_id_dates_list=test_id_dates_list,
        etl_schema=etl_schema,
    )
    if test_dates_merge_sql:
        trino_run(test_dates_merge_sql)

    # ── Immediate retry for non-permanent errors ──────────────────────────
    if raise_fail_exception and should_retry(build_results):
        log.info(
            "Errors detected that may resolve on retry — running immediate dbt retry"
        )

        retry_results = dbtRunner().invoke(["retry", "--vars", dbt_vars] + dir_args)

        retry_parsed = parse_dbt_results(
            event_dates=event_dates, run_response=retry_results
        )

        # Merge retry results — the MERGE will overwrite error rows with successes
        retry_model_merge_sql = sql_dbt_model_dates_merge(
            database_name=trino_catalog,
            etl_timestamp=etl_timestamp,
            event_dates=event_dates,
            model_id_dates_list=retry_parsed["model_id_dates_list"],
            etl_schema=etl_schema,
        )
        if retry_model_merge_sql:
            trino_run(retry_model_merge_sql)

        retry_test_merge_sql = sql_dbt_test_dates_merge(
            database_name=trino_catalog,
            etl_timestamp=etl_timestamp,
            event_dates=event_dates,
            test_id_dates_list=retry_parsed["test_id_dates_list"],
            etl_schema=etl_schema,
        )
        if retry_test_merge_sql:
            trino_run(retry_test_merge_sql)

        # Re-evaluate whether we still need to fail after the retry
        raise_fail_exception = retry_parsed["raise_fail_exception"]

    if raise_fail_exception:
        raise AirflowFailException("One or more models failed to run successfully")


def k8s_scale(workers: int):
    """
    Scale the Kubernetes deployment to the specified number of worker replicas.
    """
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
    """
    Execute a SQL statement against Trino and return the result rows.
    """
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


def send_dag_failure_notification(
    email_list: list[str], context: dict, task_instance=None, dag_name: str = None
):
    """
    Send a consolidated failure notification for a DAG run based on model and test failures from meta tables.

    Args:
        email_list: List of email addresses to send notifications to
        context: Airflow context dictionary containing dag_run and other info
        task_instance: Task instance (required for getting etl_timestamp)
        dag_name: Custom name for the DAG (optional, defaults to dag_id)
    """
    if not email_list:
        return

    dag_run = context["dag_run"]

    # Get etl_timestamp from task_instance - required for querying meta tables
    etl_timestamp = None
    if task_instance:
        try:
            etl_timestamp = task_instance.xcom_pull(
                key="etl_timestamp", task_ids="start_etl"
            )
        except:
            log.warning("Could not retrieve etl_timestamp from task_instance")
            return

    if not etl_timestamp:
        log.warning("etl_timestamp is required to query meta tables for failures")
        return

    # Query failed models from dbt_model_dates table (distinct models only)
    failed_models_sql = f"""
        SELECT
            model_id,
            run_status,
            substring(run_message,1,100) AS run_message,
            array_join(
                array_agg(
                    CAST(event_date AS VARCHAR) 
                    ORDER BY event_date
                ), 
                ', ' 
            ) AS event_date
        FROM
            {trino_catalog}.{etl_schema}.dbt_model_dates
        WHERE
            etl_timestamp = cast('{etl_timestamp}' as timestamp(6))
            AND run_status != 'success'
        GROUP BY
            model_id,
            run_status,
            substring(run_message,1,100)
        ORDER BY
            model_id
    """

    failed_models = trino_run(failed_models_sql)

    # Query failed tests from dbt_test_dates table (distinct test_id, model_id combinations)
    failed_tests_sql = f"""
        SELECT DISTINCT
            test_id,
            model_id,
            test_status,
            substring(test_message,1,100) as test_message
        FROM
            {trino_catalog}.{etl_schema}.dbt_test_dates
        WHERE
            etl_timestamp = cast('{etl_timestamp}' as timestamp(6))
            AND test_status != 'pass'
        ORDER BY
            model_id, test_id
    """

    failed_tests = trino_run(failed_tests_sql)

    # Send consolidated failure notification if there are failures
    if failed_models or failed_tests:
        subject = f"{dag_run.dag_id} DAG Failures - {dag_run.execution_date.strftime('%Y-%m-%d %H:%M:%S')}"

        html_content = f"""
        <h2>Model and Test Failure Summary</h2>
        <p><strong>DAG ID:</strong> {dag_run.dag_id}</p>
        <p><strong>Execution Date:</strong> {dag_run.execution_date}</p>
        <p><strong>ETL Timestamp:</strong> {etl_timestamp}</p>
        """

        # Add failed models section
        if failed_models:
            total_failed_models = len(failed_models)
            html_content += f"""
            <h3>Failed/Skipped Models ({total_failed_models}):</h3>
            <table border="1" cellpadding="5" cellspacing="0">
                <tr>
                    <th>Model Name</th>
                    <th>Run Status</th>
                    <th>Event Date</th>
                    <th>Error Message</th>
                </tr>
                """

            for model in failed_models:
                model_id = model[0] if model[0] else "N/A"
                run_status = model[1] if model[1] else "error"
                run_message = model[2] if len(model) > 2 and model[2] else "N/A"
                event_date = model[3] if len(model) > 3 and model[3] else "N/A"
                model_name = (
                    get_model_name_from_id(model_id) if model_id != "N/A" else "N/A"
                )

                # Escape HTML in messages
                run_message_escaped = (
                    str(run_message)
                    .replace("<", "&lt;")
                    .replace(">", "&gt;")
                    .replace("&", "&amp;")
                    if run_message != "N/A"
                    else "N/A"
                )

                # Color code the status
                status_color = (
                    "red"
                    if run_status.lower() == "error"
                    else "orange" if run_status.lower() == "skipped" else "black"
                )
                status_style = f"color: {status_color}; font-weight: bold;"

                html_content += f"""
                <tr>
                    <td>{model_name}</td>
                    <td style="{status_style}">{run_status}</td>
                    <td>{event_date}</td>
                    <td>{run_message_escaped}</td>
                </tr>
                """

            html_content += "</table>"

        # Add failed tests section
        if failed_tests:
            total_failed_tests = len(failed_tests)
            html_content += f"""
            <h3>Failed/Skipped Tests ({total_failed_tests}):</h3>
            <table border="1" cellpadding="5" cellspacing="0">
                <tr>
                    <th>Test ID</th>
                    <th>Model Name</th>
                    <th>Test Status</th>
                    <th>Error Message</th>
                </tr>
                """

            for test in failed_tests:
                test_id = test[0] if test[0] else "N/A"
                model_id = test[1] if test[1] else "N/A"
                test_status = test[2] if test[2] else "fail"
                test_message = test[3] if len(test) > 3 and test[3] else "N/A"
                model_name = (
                    get_model_name_from_id(model_id) if model_id != "N/A" else "N/A"
                )

                # Escape HTML in messages
                test_message_escaped = (
                    str(test_message)
                    .replace("<", "&lt;")
                    .replace(">", "&gt;")
                    .replace("&", "&amp;")
                    if test_message != "N/A"
                    else "N/A"
                )

                # Color code the status
                status_color = (
                    "red"
                    if test_status.lower() in ["fail", "error"]
                    else "orange" if test_status.lower() == "skipped" else "black"
                )
                status_style = f"color: {status_color}; font-weight: bold;"

                html_content += f"""
                <tr>
                    <td>{test_id}</td>
                    <td>{model_name}</td>
                    <td style="{status_style}">{test_status}</td>
                    <td>{test_message_escaped}</td>
                </tr>
                """

            html_content += "</table>"

        # Construct DAG run URL using task_instance or Airflow configuration
        dag_run_url = None
        if task_instance:
            try:
                # Try to get URL from task_instance's dag
                from airflow.configuration import conf

                webserver_base_url = conf.get("webserver", "BASE_URL", fallback="")
                if webserver_base_url:
                    dag_run_id = dag_run.run_id
                    # Remove trailing slash if present
                    webserver_base_url = webserver_base_url.rstrip("/")
                    dag_run_url = f"{webserver_base_url}/dags/{dag_run.dag_id}/grid?dag_run_id={dag_run_id}"
            except Exception as e:
                log.warning(f"Could not construct DAG run URL: {str(e)}")

        if dag_run_url:
            html_content += f"""
        <p>Please check the <a href="{dag_run_url}">Airflow UI</a> for detailed logs and error messages.</p>
        """
        else:
            html_content += """
        <p>Please check the Airflow UI for detailed logs and error messages.</p>
        """

        # Send the consolidated notification
        try:
            send_email(to=email_list, subject=subject, html_content=html_content)
            total_failures = len(failed_models) + len(failed_tests)
            log.info(
                f"Sent consolidated failure notification for {total_failures} failures ({len(failed_models)} models, {len(failed_tests)} tests)"
            )
        except Exception as e:
            log.error(f"Failed to send consolidated notification: {str(e)}")
            raise AirflowFailException(
                f"Failed to send consolidated notification: {str(e)}"
            )
